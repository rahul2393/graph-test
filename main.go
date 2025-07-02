package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/rahul2393/spanner-experiments/irahul-graph-test/metrics"
	"google.golang.org/api/option"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc/status"
)

var (
	VUS                    = 10                             // 并发用户数
	ZONE_START             = 100                            // 起始区ID
	ZONES_TOTAL            = 10                             // 总区数
	RECORDS_PER_ZONE       = 80                             // 每区玩家数
	EDGES_PER_RELATION     = 100                            // 每种关系的边数
	TOTAL_VERTICES         = ZONES_TOTAL * RECORDS_PER_ZONE // 总顶点数（使用固定的 IDS_PER_ZONE = 8000）
	STR_ATTR_CNT           = 10                             // 字符串属性数量
	INT_ATTR_CNT           = 90                             // 整数属性数量
	PreGenerateVertexData  = true                           // 是否预生成所有顶点数据
	ShuffleProcessingOrder = false                          // 是否随机化处理顺序以避免热点
	MaxCommitDelayMs       = 100                            // 最大提交延迟(毫秒)，用于提高写入吞吐量
	BATCH_NUM              = 1                              // 批量写入大小
	instanceID             = "irahul-load-test"
	GRAPH_NAME             = "g0618"
	credentialsFile        = "sa2.json" // GCP credentials file path
	projectID              = "span-cloud-testing"
	databaseID             = "graphdb"
)

// initFromEnv initializes configuration from environment variables
func initFromEnv() {
	// Initialize VUS from environment variable
	if vusStr := os.Getenv("VUS"); vusStr != "" {
		if parsedVUS, err := strconv.Atoi(vusStr); err == nil && parsedVUS > 0 {
			VUS = parsedVUS
		}
	}

	// Initialize ZONE_START from environment variable
	if zoneStartStr := os.Getenv("ZONE_START"); zoneStartStr != "" {
		if parsedZoneStart, err := strconv.Atoi(zoneStartStr); err == nil && parsedZoneStart >= 0 {
			ZONE_START = parsedZoneStart
		}
	}

	// Initialize ZONES_TOTAL from environment variable
	if zonesTotalStr := os.Getenv("ZONES_TOTAL"); zonesTotalStr != "" {
		if parsedZonesTotal, err := strconv.Atoi(zonesTotalStr); err == nil && parsedZonesTotal > 0 {
			ZONES_TOTAL = parsedZonesTotal
		}
	}

	// Initialize RECORDS_PER_ZONE from environment variable
	if recordsPerZoneStr := os.Getenv("RECORDS_PER_ZONE"); recordsPerZoneStr != "" {
		if parsedRecordsPerZone, err := strconv.Atoi(recordsPerZoneStr); err == nil && parsedRecordsPerZone > 0 {
			RECORDS_PER_ZONE = parsedRecordsPerZone
		}
	}

	// Initialize EDGES_PER_RELATION from environment variable
	if edgesPerRelationStr := os.Getenv("EDGES_PER_RELATION"); edgesPerRelationStr != "" {
		if parsedEdgesPerRelation, err := strconv.Atoi(edgesPerRelationStr); err == nil && parsedEdgesPerRelation > 0 {
			EDGES_PER_RELATION = parsedEdgesPerRelation
		}
	}

	// Initialize GRAPH_NAME from environment variable with default value "g0618"
	if graphNameStr := os.Getenv("GRAPH_NAME"); graphNameStr != "" {
		GRAPH_NAME = graphNameStr
	} else {
		GRAPH_NAME = "g0618"
	}
	log.Printf("GRAPH_NAME set to: %s", GRAPH_NAME)

	// Initialize PreGenerateVertexData from environment variable
	if preGenStr := os.Getenv("PRE_GENERATE_VERTEX_DATA"); preGenStr != "" {
		lower := strings.ToLower(preGenStr)
		if lower == "false" || lower == "0" {
			PreGenerateVertexData = false
		} else if lower == "true" || lower == "1" {
			PreGenerateVertexData = true
		}
	}

	// Initialize ShuffleProcessingOrder from environment variable
	if shuffleStr := os.Getenv("SHUFFLE_PROCESSING_ORDER"); shuffleStr != "" {
		lower := strings.ToLower(shuffleStr)
		if lower == "false" || lower == "0" {
			ShuffleProcessingOrder = false
		} else if lower == "true" || lower == "1" {
			ShuffleProcessingOrder = true
		}
	}

	// Initialize MaxCommitDelayMs from environment variable
	if delayStr := os.Getenv("MAX_COMMIT_DELAY_MS"); delayStr != "" {
		if parsedDelay, err := strconv.Atoi(delayStr); err == nil && parsedDelay >= 0 {
			MaxCommitDelayMs = parsedDelay
		}
	}

	// Recalculate TOTAL_VERTICES after configuration changes
	TOTAL_VERTICES = ZONES_TOTAL * RECORDS_PER_ZONE

	// Initialize instanceID from environment variable
	if instID := os.Getenv("INSTANCE_ID"); instID != "" {
		instanceID = instID
	}

	// Initialize BATCH_NUM from environment variable
	if batchNumStr := os.Getenv("BATCH_NUM"); batchNumStr != "" {
		if parsedBatchNum, err := strconv.Atoi(batchNumStr); err == nil && parsedBatchNum > 0 {
			BATCH_NUM = parsedBatchNum
		}
	}

	// Initialize credentialsFile from environment variable
	if credFile := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS_FILE"); credFile != "" {
		credentialsFile = credFile
	}

	// Initialize projectID from environment variable
	if projectIDStr := os.Getenv("PROJECT_ID"); projectIDStr != "" {
		projectID = projectIDStr
	}

	// Initialize databaseID from environment variable
	if dbID := os.Getenv("DATABASE_ID"); dbID != "" {
		databaseID = dbID
	}

	log.Printf("Configuration: VUS=%d, ZONE_START=%d, ZONES_TOTAL=%d, RECORDS_PER_ZONE=%d, EDGES_PER_RELATION=%d, TOTAL_VERTICES=%d, PreGenerateVertexData=%v, ShuffleProcessingOrder=%v, MaxCommitDelayMs=%d, BATCH_NUM=%d, credentialsFile=%s",
		VUS, ZONE_START, ZONES_TOTAL, RECORDS_PER_ZONE, EDGES_PER_RELATION, TOTAL_VERTICES, PreGenerateVertexData, ShuffleProcessingOrder, MaxCommitDelayMs, BATCH_NUM, credentialsFile)
}

func countdownOrExit(action string, seconds int) {
	log.Printf("即将%s，%d秒后启动。按 Ctrl+C 可中断...", action, seconds)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	timer := time.NewTimer(time.Duration(seconds) * time.Second)
	select {
	case <-interrupt:
		log.Println("用户中断，程序退出。")
		os.Exit(1)
	case <-timer.C:
		// Continue normally
	}
}

// generateShuffledIndices creates a shuffled list of indices to randomize processing order
func generateShuffledIndices(totalVertices int) []int {
	indices := make([]int, totalVertices)
	for i := 0; i < totalVertices; i++ {
		indices[i] = i
	}

	if ShuffleProcessingOrder {
		rand.Shuffle(len(indices), func(i, j int) {
			indices[i], indices[j] = indices[j], indices[i]
		})
		log.Printf("Shuffled processing order for %d vertices to avoid hotspots", totalVertices)
	} else {
		log.Printf("Using sequential processing order for %d vertices", totalVertices)
	}

	return indices
}

// generateVertexData generates vertex data in memory
func generateVertexData(zoneStart, zonesTotal, recordsPerZone, strAttrCnt, intAttrCnt int) ([]*VertexData, error) {
	totalRecords := zonesTotal * recordsPerZone
	data := make([]*VertexData, 0, totalRecords)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	for zoneID := zoneStart; zoneID < zoneStart+zonesTotal; zoneID++ {
		for id := 1; id <= recordsPerZone; id++ {
			uid := (int64(zoneID) << 40) | int64(id)

			// Generate string attributes
			strAttrs := make([]string, strAttrCnt)
			for i := 0; i < strAttrCnt; i++ {
				strAttrs[i] = randFixedString(rng, letters, 20)
			}

			// Generate integer attributes
			intAttrs := make([]int64, intAttrCnt)
			for i := 0; i < intAttrCnt; i++ {
				intAttrs[i] = rng.Int63n(10000)
			}

			vertex := &VertexData{
				UID:      uid,
				StrAttrs: strAttrs,
				IntAttrs: intAttrs,
			}

			data = append(data, vertex)
		}
	}

	log.Printf("Generated %d vertex records", len(data))
	return data, nil
}

func randFixedString(rng *rand.Rand, pool []rune, n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = pool[rng.Intn(len(pool))]
	}
	return string(b)
}

// generateVertexForIndex generates vertex data for a specific index
func generateVertexForIndex(index int) *VertexData {
	// Convert index to UID (existing logic)
	zoneOffset := index / RECORDS_PER_ZONE
	idInZone := index%RECORDS_PER_ZONE + 1
	zoneID := ZONE_START + zoneOffset
	uid := (int64(zoneID) << 40) | int64(idInZone)

	// Use UID as seed for consistent data generation
	rng := rand.New(rand.NewSource(uid))
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	// Generate string attributes
	strAttrs := make([]string, STR_ATTR_CNT)
	for i := 0; i < STR_ATTR_CNT; i++ {
		strAttrs[i] = randFixedString(rng, letters, 20)
	}

	// Generate integer attributes
	intAttrs := make([]int64, INT_ATTR_CNT)
	for i := 0; i < INT_ATTR_CNT; i++ {
		intAttrs[i] = rng.Int63n(10000)
	}

	return &VertexData{
		UID:      uid,
		StrAttrs: strAttrs,
		IntAttrs: intAttrs,
	}
}

// generateShuffledPlayerIndices creates a shuffled list of player indices to randomize edge processing order
func generateShuffledPlayerIndices(totalPlayers int) []int {
	indices := make([]int, totalPlayers)
	for i := 0; i < totalPlayers; i++ {
		indices[i] = i
	}

	if ShuffleProcessingOrder {
		rand.Shuffle(len(indices), func(i, j int) {
			indices[i], indices[j] = indices[j], indices[i]
		})
		log.Printf("Shuffled processing order for %d players to avoid edge hotspots", totalPlayers)
	} else {
		log.Printf("Using sequential processing order for %d players", totalPlayers)
	}

	return indices
}

// VertexData represents a vertex to be inserted
type VertexData struct {
	UID      int64
	StrAttrs []string
	IntAttrs []int64
}

// setupTableWithoutIndex creates all tables, TTL policies, and the property graph but excludes indexes.
func setupTableWithoutIndex(ctx context.Context) error {
	admin, err := database.NewDatabaseAdminClient(ctx, option.WithCredentialsFile(credentialsFile))
	if err != nil {
		if st, ok := status.FromError(err); ok {
			log.Printf("NewDatabaseAdminClient failed - Code: %v (%d), Message: %s",
				st.Code(), int(st.Code()), st.Message())
			for _, detail := range st.Details() {
				log.Printf("Error Detail: %+v", detail)
			}
		} else {
			log.Printf("NewDatabaseAdminClient failed - Error: %v", err)
		}
		return err
	}
	defer admin.Close()

	dbPath := fmt.Sprintf(
		"projects/%s/instances/%s/databases/%s",
		projectID, instanceID, databaseID,
	)

	var ddl []string

	// 1. Clean slate
	ddl = append(ddl, fmt.Sprintf("DROP PROPERTY GRAPH IF EXISTS %s", GRAPH_NAME))
	edgeLabels := []string{"Rel1", "Rel2", "Rel3", "Rel4", "Rel5"}
	for _, label := range edgeLabels {
		ddl = append(ddl, fmt.Sprintf("DROP TABLE IF EXISTS %s", label))
	}
	ddl = append(ddl, "DROP INDEX IF EXISTS user_attr11_attr12_attr13_idx")
	for _, label := range edgeLabels {
		ddl = append(ddl, fmt.Sprintf("DROP INDEX IF EXISTS %s_uid_attr_covering_idx", strings.ToLower(label)))
	}
	ddl = append(ddl, "DROP TABLE IF EXISTS Users")

	// 2. Vertex table
	ddl = append(ddl, `
CREATE TABLE Users (
  uid          INT64  NOT NULL,
  attr1        STRING(20),
  attr2        STRING(20),
  attr3        STRING(20),
  attr4        STRING(20),
  attr5        STRING(20),
  attr6        STRING(20),
  attr7        STRING(20),
  attr8        STRING(20),
  attr9        STRING(20),
  attr10       STRING(20),
  attr11       INT64,
  attr12       INT64,
  attr13       INT64,
  attr14       INT64,
  attr15       INT64,
  attr16       INT64,
  attr17       INT64,
  attr18       INT64,
  attr19       INT64,
  attr20       INT64,
  attr21       INT64,
  attr22       INT64,
  attr23       INT64,
  attr24       INT64,
  attr25       INT64,
  attr26       INT64,
  attr27       INT64,
  attr28       INT64,
  attr29       INT64,
  attr30       INT64,
  attr31       INT64,
  attr32       INT64,
  attr33       INT64,
  attr34       INT64,
  attr35       INT64,
  attr36       INT64,
  attr37       INT64,
  attr38       INT64,
  attr39       INT64,
  attr40       INT64,
  attr41       INT64,
  attr42       INT64,
  attr43       INT64,
  attr44       INT64,
  attr45       INT64,
  attr46       INT64,
  attr47       INT64,
  attr48       INT64,
  attr49       INT64,
  attr50       INT64,
  attr51       INT64,
  attr52       INT64,
  attr53       INT64,
  attr54       INT64,
  attr55       INT64,
  attr56       INT64,
  attr57       INT64,
  attr58       INT64,
  attr59       INT64,
  attr60       INT64,
  attr61       INT64,
  attr62       INT64,
  attr63       INT64,
  attr64       INT64,
  attr65       INT64,
  attr66       INT64,
  attr67       INT64,
  attr68       INT64,
  attr69       INT64,
  attr70       INT64,
  attr71       INT64,
  attr72       INT64,
  attr73       INT64,
  attr74       INT64,
  attr75       INT64,
  attr76       INT64,
  attr77       INT64,
  attr78       INT64,
  attr79       INT64,
  attr80       INT64,
  attr81       INT64,
  attr82       INT64,
  attr83       INT64,
  attr84       INT64,
  attr85       INT64,
  attr86       INT64,
  attr87       INT64,
  attr88       INT64,
  attr89       INT64,
  attr90       INT64,
  attr91       INT64,
  attr92       INT64,
  attr93       INT64,
  attr94       INT64,
  attr95       INT64,
  attr96       INT64,
  attr97       INT64,
  attr98       INT64,
  attr99       INT64,
  attr100      INT64,
  expire_time  TIMESTAMP OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (uid)`)

	// Add TTL policy for Users table
	ddl = append(ddl,
		`ALTER TABLE Users
		   ADD ROW DELETION POLICY (OLDER_THAN(expire_time, INTERVAL 8 DAY))`,
	)

	// 3. Edge tables + TTL (without indexes)
	for _, label := range edgeLabels {
		// FIXED: Renamed src_uid to uid to match parent table's PK for interleaving
		ddl = append(ddl, fmt.Sprintf(`
CREATE TABLE %s (
  uid          INT64      NOT NULL,
  dst_uid      INT64      NOT NULL,
  attr101      INT64,
  attr102      INT64,
  attr103      INT64,
  attr104      INT64,
  attr105      INT64,
  attr106      INT64,
  attr107      INT64,
  attr108      INT64,
  attr109      INT64,
  attr110      INT64,
  expire_time  TIMESTAMP OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (uid, dst_uid),
  INTERLEAVE IN PARENT Users ON DELETE CASCADE`, label))

		ddl = append(ddl, fmt.Sprintf(
			`ALTER TABLE %s
			   ADD ROW DELETION POLICY (OLDER_THAN(expire_time, INTERVAL 8 DAY))`,
			label))
	}

	// 4. Property graph definition
	var edgeDefs []string
	for _, l := range edgeLabels {
		// FIXED: SOURCE KEY now correctly references 'uid'
		edgeDefs = append(edgeDefs, fmt.Sprintf(`
  %s
    SOURCE KEY (uid) REFERENCES Users(uid)
    DESTINATION KEY (dst_uid) REFERENCES Users(uid)
    LABEL %s PROPERTIES (attr101, attr102, attr103, attr104, attr105, attr106, attr107, attr108, attr109, attr110)`, l, l))
	}

	userProps := []string{"uid"}
	for i := 1; i <= 100; i++ {
		userProps = append(userProps, fmt.Sprintf("attr%d", i))
	}

	graphDDL := fmt.Sprintf(`CREATE PROPERTY GRAPH %s
NODE TABLES (
  Users KEY (uid)
    LABEL User PROPERTIES (%s)
)
EDGE TABLES (%s
)`, GRAPH_NAME, strings.Join(userProps, ", "), strings.Join(edgeDefs, ","))

	ddl = append(ddl, graphDDL)

	// print all the ddl to the terminal
	log.Println("Tables and Property Graph DDL:")
	log.Println(strings.Join(ddl, "\n\n"))

	time.Sleep(100 * time.Second)

	// 5. Push DDL to Spanner
	op, err := admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   dbPath,
		Statements: ddl,
	})
	if err != nil {
		if st, ok := status.FromError(err); ok {
			log.Printf("UpdateDatabaseDdl failed - Code: %v (%d), Message: %s",
				st.Code(), int(st.Code()), st.Message())
			for _, detail := range st.Details() {
				log.Printf("Error Detail: %+v", detail)
			}
		} else {
			log.Printf("UpdateDatabaseDdl failed - Error: %v", err)
		}
		return err
	}
	if err := op.Wait(ctx); err != nil {
		if st, ok := status.FromError(err); ok {
			log.Printf("DDL operation wait failed - Code: %v (%d), Message: %s",
				st.Code(), int(st.Code()), st.Message())
			for _, detail := range st.Details() {
				log.Printf("Error Detail: %+v", detail)
			}
		} else {
			log.Printf("DDL operation wait failed - Error: %v", err)
		}
		return err
	}

	log.Printf("Tables and graph %q created successfully in %s", GRAPH_NAME, dbPath)
	return nil
}

// setupAllTableIndexes creates all indexes for the tables.
func setupAllTableIndexes(ctx context.Context) error {
	admin, err := database.NewDatabaseAdminClient(ctx, option.WithCredentialsFile(credentialsFile))
	if err != nil {
		if st, ok := status.FromError(err); ok {
			log.Printf("NewDatabaseAdminClient failed - Code: %v (%d), Message: %s",
				st.Code(), int(st.Code()), st.Message())
			for _, detail := range st.Details() {
				log.Printf("Error Detail: %+v", detail)
			}
		} else {
			log.Printf("NewDatabaseAdminClient failed - Error: %v", err)
		}
		return err
	}
	defer admin.Close()

	dbPath := fmt.Sprintf(
		"projects/%s/instances/%s/databases/%s",
		projectID, instanceID, databaseID,
	)

	var ddl []string
	edgeLabels := []string{"Rel1", "Rel2", "Rel3", "Rel4", "Rel5"}

	// Create index for Users table
	ddl = append(ddl,
		`CREATE INDEX user_attr11_attr12_attr13_idx
		   ON Users(attr11, attr12, attr13)`,
	)

	// Create indexes for edge tables
	for _, label := range edgeLabels {
		ddl = append(ddl, fmt.Sprintf(
			`CREATE INDEX %s_uid_attr_covering_idx
			   ON %s(uid, attr101, attr102, attr103)`,
			strings.ToLower(label), label))
	}

	// print all the ddl to the terminal
	log.Println("Index DDL:")
	log.Println(strings.Join(ddl, "\n\n"))

	time.Sleep(100 * time.Second)

	// Push DDL to Spanner
	op, err := admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   dbPath,
		Statements: ddl,
	})
	if err != nil {
		if st, ok := status.FromError(err); ok {
			log.Printf("UpdateDatabaseDdl failed - Code: %v (%d), Message: %s",
				st.Code(), int(st.Code()), st.Message())
			for _, detail := range st.Details() {
				log.Printf("Error Detail: %+v", detail)
			}
		} else {
			log.Printf("UpdateDatabaseDdl failed - Error: %v", err)
		}
		return err
	}
	if err := op.Wait(ctx); err != nil {
		if st, ok := status.FromError(err); ok {
			log.Printf("DDL operation wait failed - Code: %v (%d), Message: %s",
				st.Code(), int(st.Code()), st.Message())
			for _, detail := range st.Details() {
				log.Printf("Error Detail: %+v", detail)
			}
		} else {
			log.Printf("DDL operation wait failed - Error: %v", err)
		}
		return err
	}

	log.Printf("All indexes created successfully in %s", dbPath)
	return nil
}

// setupGraphSpanner creates all tables, indexes, TTL policies, and the property graph.
func setupGraphSpanner(ctx context.Context) error {
	// First create tables without indexes
	if err := setupTableWithoutIndex(ctx); err != nil {
		return err
	}

	// Then create all indexes
	if err := setupAllTableIndexes(ctx); err != nil {
		return err
	}

	return nil
}

func spannerWriteBatchVertexTest(client *spanner.Client, batchNum int) {
	log.Printf("Starting Spanner batch vertex write test with batch size %d...", batchNum)

	// Set up graceful shutdown signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	go func() {
		<-interrupt
		log.Println("\nReceived interrupt signal, stopping workers gracefully...")
		cancel()
	}()

	var wg sync.WaitGroup
	startTime := time.Now()

	// Initialize metrics
	metricsCollector := metrics.NewConcurrentMetrics(VUS)

	totalVertices := ZONES_TOTAL * RECORDS_PER_ZONE
	verticesPerWorker := int(math.Ceil(float64(totalVertices) / float64(VUS)))

	// Generate shuffled indices for randomized processing order
	processingIndices := generateShuffledIndices(totalVertices)

	// Configure max commit delay for throughput optimization
	var applyOpts []spanner.ApplyOption
	if MaxCommitDelayMs > 0 {
		maxDelay := time.Duration(MaxCommitDelayMs) * time.Millisecond
		commitOpts := spanner.CommitOptions{MaxCommitDelay: &maxDelay}
		applyOpts = []spanner.ApplyOption{
			spanner.ApplyCommitOptions(commitOpts),
		}
		log.Printf("Using max commit delay: %dms for throughput optimization", MaxCommitDelayMs)
	} else {
		log.Println("Max commit delay disabled (0ms)")
	}

	log.Printf("Workers will generate vertices on-the-fly. Total: %d vertices, %d workers, batch size: %d...", totalVertices, VUS, batchNum)

	log.Printf("Starting %d write workers...", VUS)
	log.Println("Press Ctrl+C at any time to stop gracefully...")
	countdownOrExit("开始批量写入顶点", 5)

	for worker := 0; worker < VUS; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Calculate data range for this worker
			startIdx := workerID * verticesPerWorker
			endIdx := (workerID + 1) * verticesPerWorker
			if endIdx > totalVertices {
				endIdx = totalVertices
			}

			log.Printf("Worker %d started, processing vertices %d to %d", workerID, startIdx, endIdx-1)

			workerSuccessCount := 0
			workerErrorCount := 0

			// Batch mutations
			var mutations []*spanner.Mutation

			// Process vertices assigned to this worker
			for i := startIdx; i < endIdx; i++ {
				// Check for cancellation
				select {
				case <-ctx.Done():
					log.Printf("Worker %d stopping due to interrupt (processed %d/%d vertices)",
						workerID, i-startIdx, endIdx-startIdx)
					return
				default:
				}

				// Get the actual index to process (potentially shuffled)
				actualIndex := processingIndices[i]

				// Generate vertex on-the-fly using the shuffled index
				vertex := generateVertexForIndex(actualIndex)

				// Build mutation for this vertex
				mutation := buildVertexMutation(vertex)
				mutations = append(mutations, mutation)

				// Execute batch when reaching batchNum or at the end
				if len(mutations) >= batchNum || i == endIdx-1 {
					insertStart := time.Now()
					_, err := client.Apply(ctx, mutations, applyOpts...)
					insertDuration := time.Since(insertStart)

					// Record metrics for the batch
					metricsCollector.AddDuration(workerID, insertDuration)

					if err != nil {
						if ctx.Err() != nil {
							// Context was cancelled, exit gracefully
							log.Printf("Worker %d stopping due to context cancellation", workerID)
							return
						}
						log.Printf("Worker %d batch insert failed, batch size %d: %s", workerID, len(mutations), err.Error())
						workerErrorCount += len(mutations)
						metricsCollector.AddError(int64(len(mutations)))
					} else {
						workerSuccessCount += len(mutations)
						metricsCollector.AddSuccess(int64(len(mutations)))
					}

					// Reset mutations slice for next batch
					mutations = []*spanner.Mutation{}
				}

				// Log progress every 100 inserts
				if (i-startIdx+1)%100 == 0 {
					log.Printf("Worker %d processed %d vertices, success: %d, errors: %d",
						workerID, i-startIdx+1, workerSuccessCount, workerErrorCount)
				}
			}

			log.Printf("Worker %d completed: %d success, %d errors",
				workerID, workerSuccessCount, workerErrorCount)
		}(worker)
	}

	// Wait for all workers to complete or be interrupted
	wg.Wait()
	totalDuration := time.Since(startTime)

	// Get combined metrics
	combinedMetrics := metricsCollector.CombinedStats()
	totalSuccess := metricsCollector.GetSuccessCount()
	totalErrors := metricsCollector.GetErrorCount()

	if ctx.Err() != nil {
		log.Println("Batch vertex write test interrupted by user:")
	} else {
		log.Println("Batch vertex write test completed:")
	}
	log.Printf("  Total duration: %v", totalDuration)
	log.Printf("  Total vertices: %d", totalVertices)
	log.Printf("  Batch size: %d", batchNum)
	log.Printf("  Successful inserts: %d", totalSuccess)
	log.Printf("  Failed inserts: %d", totalErrors)
	if totalVertices > 0 {
		log.Printf("  Success rate: %.2f%%", float64(totalSuccess)*100/float64(totalVertices))
	}
	log.Printf("  Throughput: %.2f vertices/sec", float64(totalSuccess)/totalDuration.Seconds())
	log.Printf("  Latency metrics: %s", combinedMetrics.String())
}

// buildVertexMutation builds a Spanner mutation for inserting a vertex
func buildVertexMutation(vertex *VertexData) *spanner.Mutation {
	// Prepare columns and values for the Users table
	columns := []string{"uid"}
	values := []interface{}{vertex.UID}

	// Add string attributes (attr1-attr10)
	for i, strAttr := range vertex.StrAttrs {
		columns = append(columns, fmt.Sprintf("attr%d", i+1))
		values = append(values, strAttr)
	}

	// Add integer attributes (attr11-attr100)
	for i, intAttr := range vertex.IntAttrs {
		attrIndex := len(vertex.StrAttrs) + i + 1
		columns = append(columns, fmt.Sprintf("attr%d", attrIndex))
		values = append(values, intAttr)
	}

	// Add expire_time for TTL
	columns = append(columns, "expire_time")
	values = append(values, spanner.CommitTimestamp)

	return spanner.Insert("Users", columns, values)
}

// spannerWriteVertexTest performs vertex write testing with multiple goroutines
func spannerWriteVertexTest(client *spanner.Client, preGenerate bool) {
	log.Println("Starting Spanner vertex write test...")

	// Set up graceful shutdown signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	go func() {
		<-interrupt
		log.Println("\nReceived interrupt signal, stopping workers gracefully...")
		cancel()
	}()

	var wg sync.WaitGroup
	startTime := time.Now()

	// Initialize metrics
	metricsCollector := metrics.NewConcurrentMetrics(VUS)

	totalVertices := ZONES_TOTAL * RECORDS_PER_ZONE
	verticesPerWorker := int(math.Ceil(float64(totalVertices) / float64(VUS)))

	// Generate shuffled indices for randomized processing order
	processingIndices := generateShuffledIndices(totalVertices)

	var vertices []*VertexData

	if preGenerate {
		// Step 1: Generate vertex data in memory (original behavior)
		log.Println("Generating vertex data in memory...")
		var err error
		vertices, err = generateVertexData(ZONE_START, ZONES_TOTAL, RECORDS_PER_ZONE, STR_ATTR_CNT, INT_ATTR_CNT)
		if err != nil {
			log.Printf("Failed to generate vertex data: %s", err.Error())
			return
		}
		log.Printf("Distributing %d vertices among %d workers...", len(vertices), VUS)
	} else {
		log.Printf("Workers will generate vertices on-the-fly. Total: %d vertices, %d workers...", totalVertices, VUS)
	}

	log.Printf("Starting %d write workers...", VUS)
	log.Println("Press Ctrl+C at any time to stop gracefully...")
	countdownOrExit("开始写入顶点", 5)

	for worker := 0; worker < VUS; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Calculate data range for this worker
			startIdx := workerID * verticesPerWorker
			endIdx := (workerID + 1) * verticesPerWorker
			if endIdx > totalVertices {
				endIdx = totalVertices
			}

			log.Printf("Worker %d started, processing vertices %d to %d", workerID, startIdx, endIdx-1)

			workerSuccessCount := 0
			workerErrorCount := 0

			// Process vertices assigned to this worker
			for i := startIdx; i < endIdx; i++ {
				// Check for cancellation
				select {
				case <-ctx.Done():
					log.Printf("Worker %d stopping due to interrupt (processed %d/%d vertices)",
						workerID, i-startIdx, endIdx-startIdx)
					return
				default:
				}

				// Get the actual index to process (potentially shuffled)
				actualIndex := processingIndices[i]
				var vertex *VertexData

				if preGenerate {
					// Use pre-generated data
					vertex = vertices[actualIndex]
				} else {
					// Generate vertex on-the-fly using the shuffled index
					vertex = generateVertexForIndex(actualIndex)
				}

				// Build mutation for this vertex
				mutation := buildVertexMutation(vertex)

				// Execute insert
				insertStart := time.Now()
				_, err := client.Apply(ctx, []*spanner.Mutation{mutation})
				insertDuration := time.Since(insertStart)

				// Record metrics
				metricsCollector.AddDuration(workerID, insertDuration)

				if err != nil {
					if ctx.Err() != nil {
						// Context was cancelled, exit gracefully
						log.Printf("Worker %d stopping due to context cancellation", workerID)
						return
					}
					log.Printf("Worker %d insert failed for UID %d: %s", workerID, vertex.UID, err.Error())
					workerErrorCount++
					metricsCollector.AddError(1)
				} else {
					workerSuccessCount++
					metricsCollector.AddSuccess(1)
				}

				// Log progress every 100 inserts
				if (i-startIdx+1)%100 == 0 {
					log.Printf("Worker %d processed %d vertices, success: %d, errors: %d",
						workerID, i-startIdx+1, workerSuccessCount, workerErrorCount)
				}
			}

			log.Printf("Worker %d completed: %d success, %d errors",
				workerID, workerSuccessCount, workerErrorCount)
		}(worker)
	}

	// Wait for all workers to complete or be interrupted
	wg.Wait()
	totalDuration := time.Since(startTime)

	// Get combined metrics
	combinedMetrics := metricsCollector.CombinedStats()
	totalSuccess := metricsCollector.GetSuccessCount()
	totalErrors := metricsCollector.GetErrorCount()

	if ctx.Err() != nil {
		log.Println("Vertex write test interrupted by user:")
	} else {
		log.Println("Vertex write test completed:")
	}
	log.Printf("  Total duration: %v", totalDuration)
	log.Printf("  Total vertices: %d", totalVertices)
	log.Printf("  Successful inserts: %d", totalSuccess)
	log.Printf("  Failed inserts: %d", totalErrors)
	if totalVertices > 0 {
		log.Printf("  Success rate: %.2f%%", float64(totalSuccess)*100/float64(totalVertices))
	}
	log.Printf("  Throughput: %.2f vertices/sec", float64(totalSuccess)/totalDuration.Seconds())
	log.Printf("  Latency metrics: %s", combinedMetrics.String())
}

func main() {
	initFromEnv()
	// Define command line flags
	var testType string
	var startZone, endZone int
	var batchNum int
	flag.StringVar(&testType, "test", "setup", "Test type to run: setup, setupindex, write-vertex, write-edge, relation, all")
	flag.IntVar(&startZone, "start-zone", ZONE_START, "Start zone ID for edge test")
	flag.IntVar(&endZone, "end-zone", ZONE_START+ZONES_TOTAL, "End zone ID for edge test")
	flag.IntVar(&batchNum, "batch-num", EDGES_PER_RELATION, "Number of edges per batch for edge write test")
	flag.Parse()

	ctx := context.Background()

	// Fully-qualified database name:
	//   projects/{PROJECT}/instances/{INSTANCE}/databases/{DATABASE}
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)
	os.Setenv("GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS", "true")
	os.Setenv("GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS_FOR_RW", "true")
	os.Setenv("SPANNER_DISABLE_BUILTIN_METRICS", "true")
	client, err := spanner.NewClient(ctx, dbPath, option.WithCredentialsFile(credentialsFile))
	if err != nil {
		log.Fatalf("Failed to create Spanner client: %v", err)
	}
	defer client.Close()

	// Execute tests based on command line option
	switch testType {
	case "setup":
		log.Println("Running setup test...")
		// Setup graph schema
		if err := setupGraphSpanner(ctx); err != nil {
			log.Fatalf("Failed to setup graph: %v", err)
		}

	case "setupindex":
		log.Println("Running setup indexes test...")
		if err := setupAllTableIndexes(ctx); err != nil {
			log.Fatalf("Failed to setup indexes: %v", err)
		}

	case "write-vertex":
		log.Println("Running vertex write test...")
		if BATCH_NUM > 1 {
			log.Printf("Using batch mode with batch size %d", BATCH_NUM)
			spannerWriteBatchVertexTest(client, BATCH_NUM)
		} else {
			log.Println("Using single insert mode")
			spannerWriteVertexTest(client, PreGenerateVertexData)
		}

	//case "write-edge":
	//	log.Printf("Running edge write test for zones [%d, %d)...", startZone, endZone)
	//	spannerWriteEdgeTest(client, startZone, endZone, batchNum)
	//
	//case "read-relation":
	//	log.Println("Running relation read test...")
	//	spannerReadRelationTest(ctx, dbPath)
	//
	//case "read-vertex":
	//	log.Println("Running vertex read test...")
	//	spannerReadVertexTest(client)
	//
	//case "all":
	//	log.Println("Running all tests...")
	//
	//	// First setup the graph if needed
	//	if err := setupGraphSpanner(ctx); err != nil {
	//		log.Printf("Setup failed: %v", err)
	//	}
	//
	//	// Test vertex write performance
	//	spannerWriteVertexTest(client, PreGenerateVertexData)
	//
	//	// Test edge write performance
	//	spannerWriteEdgeTest(client, ZONE_START, ZONE_START+ZONES_TOTAL, batchNum)
	//
	//	// Test relation read performance
	//	spannerReadRelationTest(ctx, dbPath)

	default:
		log.Printf("Unknown test type: %s", testType)
		log.Println("Available test types: setup, setupindex, write-vertex, write-edge, relation, all")
		os.Exit(1)
	}

	log.Println("Benchmark completed")
}
