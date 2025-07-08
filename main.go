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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/rahul2393/spanner-experiments/irahul-graph-test/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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
	TEST_DURATION_SEC      = 0                              // 测试持续时间(秒)，0表示使用顶点数量模式
	UseStaleReads          = false                          // 是否使用过期读取
	StaleReadMode          = "max"                          // 过期读取模式: max, exact
	StalenessMs            = 15000                          // 过期读取时间(毫秒)
	instanceID             = "irahul-load-test"
	GRAPH_NAME             = "g0618"
	credentialsFile        = "sa2.json"                     // GCP credentials file path
	projectID              = "span-cloud-testing"
	databaseID             = "graphdb"

	// Atomic counters for metrics
	totalInserts   uint64 = 0
	successInserts uint64 = 0
	errorInserts   uint64 = 0
	currentTPS     uint64 = 0
)

const gfeT4T7prefix = "gfet4t7; dur="
const serverTimingKey = "server-timing"

var serverTimingPattern = regexp.MustCompile(`([a-zA-Z0-9_-]+);\s*dur=(\d*\.?\d+)`)

// parseT4T7Latency parse the headers and trailers for finding the gfet4t7 latency.
func parseT4T7Latency(md metadata.MD) (string, time.Duration, error) {
	if md == nil {
		return "", 0, fmt.Errorf("server-timing headers not found")
	}

	serverTiming := md.Get(serverTimingKey)

	if len(serverTiming) == 0 {
		return "", 0, fmt.Errorf("server-timing headers not found")
	}
	for _, timing := range serverTiming {
		matches := serverTimingPattern.FindAllStringSubmatch(timing, -1)
		for _, match := range matches {
			if len(match) == 3 { // full match + 2 capture groups
				metricName := match[1]
				duration, err := strconv.ParseFloat(match[2], 10)
				if err != nil {
					return "", 0, fmt.Errorf("failed to parse gfe latency: %v", err)

				}
				if metricName == "gfet4t7" {
					return "gfe", time.Duration(duration*1000) * time.Microsecond, nil
				}
				if metricName == "afe" {
					return "afe", time.Duration(duration*1000) * time.Microsecond, nil
				}
			}
		}
	}
	return "", 0, fmt.Errorf("no gfe latency response available")
}

func initOpenTelemetryTracer() func() {
	// Create Jaeger exporter for OpenTelemetry
	exporter, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint("http://localhost:14268/api/traces")))
	if err != nil {
		log.Printf("Failed to create Jaeger exporter: %v", err)
		panic(err)
	}

	// Create trace provider with batch span processor
	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		//trace.WithSampler(trace.AlwaysSample()),
		trace.WithSampler(trace.NeverSample()),
		//trace.WithSampler(trace.TraceIDRatioBased(0.001)), // Sample all traces for debugging
	)

	// Set the global trace provider
	otel.SetTracerProvider(tp)

	// Set global propagator for context propagation (this is key for internal spans!)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	log.Printf("OpenTelemetry tracer configured with Jaeger exporter at http://localhost:14268/api/traces")
	log.Printf("All Spanner client internal tracing will be automatically exported")

	return func() {
		log.Printf("Shutting down trace provider...")
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down trace provider: %v", err)
		}
	}
}

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

	// Initialize TEST_DURATION_SEC from environment variable
	if testDurationStr := os.Getenv("TEST_DURATION_SEC"); testDurationStr != "" {
		if parsedTestDuration, err := strconv.Atoi(testDurationStr); err == nil && parsedTestDuration >= 0 {
			TEST_DURATION_SEC = parsedTestDuration
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

	log.Printf("Configuration: VUS=%d, ZONE_START=%d, ZONES_TOTAL=%d, RECORDS_PER_ZONE=%d, EDGES_PER_RELATION=%d, TOTAL_VERTICES=%d, PreGenerateVertexData=%v, ShuffleProcessingOrder=%v, MaxCommitDelayMs=%d, BATCH_NUM=%d, TEST_DURATION_SEC=%d, credentialsFile=%s",
		VUS, ZONE_START, ZONES_TOTAL, RECORDS_PER_ZONE, EDGES_PER_RELATION, TOTAL_VERTICES, PreGenerateVertexData, ShuffleProcessingOrder, MaxCommitDelayMs, BATCH_NUM, TEST_DURATION_SEC, credentialsFile)
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

// handleInterrupt sets up graceful shutdown on interrupt signal
func handleInterrupt(cancel context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		log.Println("\nReceived interrupt signal, stopping workers gracefully...")
		cancel()
	}()
}

// startTPSMonitor starts a goroutine that logs TPS every second
func startTPSMonitor(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				tps := atomic.SwapUint64(&currentTPS, 0) // get and reset
				total := atomic.LoadUint64(&totalInserts)
				success := atomic.LoadUint64(&successInserts)
				errors := atomic.LoadUint64(&errorInserts)

				log.Printf("TPS: %d | Total: %d | Success: %d | Errors: %d",
					tps, total, success, errors)
			case <-ctx.Done():
				return
			}
		}
	}()
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

func spannerDeleteVertexUsingPDML(_ context.Context, client *spanner.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()
	count, err := client.PartitionedUpdate(ctx, spanner.NewStatement("DELETE FROM Users WHERE 1 = 1"))
	if err != nil {
		log.Fatalf("Failed to delete all vertices: %v", err)
	}
	log.Printf("Successfully deleted %d vertices using PDML", count)
	return nil
}

func spannerWriteBatchVertexTest(client *spanner.Client, batchNum int) {
	log.Printf("Starting Spanner batch vertex write test with batch size %d...", batchNum)

	// Set up graceful shutdown signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handleInterrupt(cancel)
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

	handleInterrupt(cancel)
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
				_, err := client.Apply(ctx, []*spanner.Mutation{mutation}, spanner.ApplyAtLeastOnce())
				insertDuration := time.Since(insertStart)

				// Record metrics
				metricsCollector.AddDuration(workerID, insertDuration)
				if err != nil {
					panic(err)
				}

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

// spannerWriteVertexTestImproved performs improved vertex write testing using errgroup and duration-based testing
func spannerWriteVertexTestImproved(client *spanner.Client, preGenerate bool) {
	log.Println("Starting improved Spanner vertex write test...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handleInterrupt(cancel)
	startTPSMonitor(ctx)

	startTime := time.Now()

	// Initialize detailed metrics collector
	metricsCollector := metrics.NewConcurrentMetrics(VUS)

	// Reset atomic counters
	atomic.StoreUint64(&totalInserts, 0)
	atomic.StoreUint64(&successInserts, 0)
	atomic.StoreUint64(&errorInserts, 0)
	atomic.StoreUint64(&currentTPS, 0)

	totalVertices := ZONES_TOTAL * RECORDS_PER_ZONE
	processingIndices := generateShuffledIndices(totalVertices)

	var vertices []*VertexData
	if preGenerate {
		log.Println("Generating vertex data in memory...")
		var err error
		vertices, err = generateVertexData(ZONE_START, ZONES_TOTAL, RECORDS_PER_ZONE, STR_ATTR_CNT, INT_ATTR_CNT)
		if err != nil {
			log.Printf("Failed to generate vertex data: %s", err.Error())
			return
		}
		log.Printf("Generated %d vertices in memory", len(vertices))
	}

	// Determine test mode
	isDurationBased := TEST_DURATION_SEC > 0
	if isDurationBased {
		log.Printf("Running duration-based test: %d seconds with %d workers", TEST_DURATION_SEC, VUS)
	} else {
		log.Printf("Running count-based test: %d vertices with %d workers", totalVertices, VUS)
	}

	log.Println("Press Ctrl+C at any time to stop gracefully...")
	countdownOrExit("开始写入顶点", 5)

	group, grpCtx := errgroup.WithContext(ctx)

	// Launch workers
	for workerID := 0; workerID < VUS; workerID++ {
		workerID := workerID // capture loop variable
		group.Go(func() error {
			return writeVertexWorkerWithMetrics(grpCtx, client, workerID, vertices, processingIndices, totalVertices, preGenerate, isDurationBased, metricsCollector)
		})
	}

	// Wait for all workers to complete
	if err := group.Wait(); err != nil {
		log.Printf("Worker error: %v", err)
	}

	totalDuration := time.Since(startTime)
	final_total := atomic.LoadUint64(&totalInserts)
	final_success := atomic.LoadUint64(&successInserts)
	final_errors := atomic.LoadUint64(&errorInserts)

	// Get combined metrics for detailed latency breakdown
	combinedMetrics := metricsCollector.CombinedStats()
	metricsSuccess := metricsCollector.GetSuccessCount()
	metricsErrors := metricsCollector.GetErrorCount()

	if ctx.Err() != nil {
		log.Println("Improved vertex write test interrupted by user:")
	} else {
		log.Println("Improved vertex write test completed:")
	}
	log.Printf("  Total duration: %v", totalDuration)
	log.Printf("  Total attempts: %d", final_total)
	log.Printf("  Successful inserts: %d (metrics: %d)", final_success, metricsSuccess)
	log.Printf("  Failed inserts: %d (metrics: %d)", final_errors, metricsErrors)
	if final_total > 0 {
		log.Printf("  Success rate: %.2f%%", float64(final_success)*100/float64(final_total))
	}
	log.Printf("  Throughput: %.2f vertices/sec", float64(final_success)/totalDuration.Seconds())
	log.Printf("  Latency metrics: %s", combinedMetrics.String())
}

// spannerWriteEdgeTest performs improved edge write testing using errgroup and duration-based testing
func spannerWriteEdgeTest(client *spanner.Client, startZoneID, endZoneID int, batchNum int) {
	log.Printf("Starting improved Spanner edge write test for zones [%d, %d) with batch size %d...", startZoneID, endZoneID, batchNum)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handleInterrupt(cancel)
	startTPSMonitor(ctx)

	startTime := time.Now()

	// Initialize detailed metrics collector
	metricsCollector := metrics.NewConcurrentMetrics(VUS)

	// Reset atomic counters for edge metrics
	atomic.StoreUint64(&totalInserts, 0)
	atomic.StoreUint64(&successInserts, 0)
	atomic.StoreUint64(&errorInserts, 0)
	atomic.StoreUint64(&currentTPS, 0)

	// Calculate total zones and players
	totalZones := endZoneID - startZoneID
	totalPlayers := int64(totalZones * RECORDS_PER_ZONE)
	totalEdges := totalPlayers * 5 * int64(EDGES_PER_RELATION) // 5种关系，每种边数由EDGES_PER_RELATION配置

	log.Printf("Total zones: %d, players per zone: %d", totalZones, RECORDS_PER_ZONE)
	log.Printf("Total players: %d, Total edges to insert: %d", totalPlayers, totalEdges)

	// Generate shuffled player indices for randomized processing order
	playerProcessingIndices := generateShuffledPlayerIndices(int(totalPlayers))

	// Determine test mode
	isDurationBased := TEST_DURATION_SEC > 0
	if isDurationBased {
		log.Printf("Running duration-based edge test: %d seconds with %d workers", TEST_DURATION_SEC, VUS)
	} else {
		log.Printf("Running count-based edge test: %d edges with %d workers", totalEdges, VUS)
	}

	log.Println("Press Ctrl+C at any time to stop gracefully...")
	countdownOrExit("开始写入边", 5)

	group, grpCtx := errgroup.WithContext(ctx)

	// Launch edge write workers
	for workerID := 0; workerID < VUS; workerID++ {
		workerID := workerID // capture loop variable
		group.Go(func() error {
			return writeEdgeWorkerWithMetrics(grpCtx, client, workerID, playerProcessingIndices,
				startZoneID, endZoneID, batchNum, isDurationBased, metricsCollector)
		})
	}

	// Wait for all workers to complete
	if err := group.Wait(); err != nil {
		log.Printf("Edge worker error: %v", err)
	}

	totalDuration := time.Since(startTime)
	final_total := atomic.LoadUint64(&totalInserts)
	final_success := atomic.LoadUint64(&successInserts)
	final_errors := atomic.LoadUint64(&errorInserts)

	// Get combined metrics for detailed latency breakdown
	combinedMetrics := metricsCollector.CombinedStats()
	metricsSuccess := metricsCollector.GetSuccessCount()
	metricsErrors := metricsCollector.GetErrorCount()

	if ctx.Err() != nil {
		log.Println("Improved edge write test interrupted by user:")
	} else {
		log.Println("Improved edge write test completed:")
	}
	log.Printf("  Total duration: %v", totalDuration)
	log.Printf("  Total attempts: %d", final_total)
	log.Printf("  Expected edges: %d", totalEdges)
	log.Printf("  Successful inserts: %d (metrics: %d)", final_success, metricsSuccess)
	log.Printf("  Failed inserts: %d (metrics: %d)", final_errors, metricsErrors)
	if final_total > 0 {
		log.Printf("  Success rate: %.2f%%", float64(final_success)*100/float64(final_total))
	}
	log.Printf("  Throughput: %.2f edges/sec", float64(final_success)/totalDuration.Seconds())
	log.Printf("  Latency metrics: %s", combinedMetrics.String())
}

// buildEdgeMutation builds a Spanner mutation for inserting an edge
func buildEdgeMutation(relType string, sourceUID, targetUID int64, attrs []int64) *spanner.Mutation {
	// Prepare columns and values for the edge table
	columns := []string{"uid", "dst_uid"}
	values := []interface{}{sourceUID, targetUID}

	// Add edge attributes attr101-attr110
	for i, attr := range attrs {
		columns = append(columns, fmt.Sprintf("attr%d", 101+i))
		values = append(values, attr)
	}

	// Add expire_time for TTL
	columns = append(columns, "expire_time")
	values = append(values, spanner.CommitTimestamp)

	return spanner.Insert(relType, columns, values)
}

// writeVertexWorkerWithMetrics is a worker function for writing vertices with detailed metrics collection
func writeVertexWorkerWithMetrics(ctx context.Context, client *spanner.Client, workerID int, vertices []*VertexData, processingIndices []int, totalVertices int, preGenerate bool, isDurationBased bool, metricsCollector *metrics.ConcurrentMetrics) error {
	log.Printf("Worker %d started", workerID)

	var stopTimer *time.Timer
	var done bool = false

	if isDurationBased {
		stopTimer = time.NewTimer(time.Duration(TEST_DURATION_SEC) * time.Second)
		defer func() {
			if stopTimer != nil {
				stopTimer.Stop()
			}
		}()

		go func() {
			select {
			case <-stopTimer.C:
				log.Printf("Worker %d completed due to timer", workerID)
				done = true
			case <-ctx.Done():
				log.Printf("Worker %d completed due to context cancellation", workerID)
				done = true
			}
		}()
	} else {
		// Count-based mode: calculate range for this worker
		verticesPerWorker := int(math.Ceil(float64(totalVertices) / float64(VUS)))
		startIdx := workerID * verticesPerWorker
		endIdx := (workerID + 1) * verticesPerWorker
		if endIdx > totalVertices {
			endIdx = totalVertices
		}
		log.Printf("Worker %d processing vertices %d to %d", workerID, startIdx, endIdx-1)
	}

	workerSuccessCount := 0
	workerErrorCount := 0
	var vertexIndex int = 0

	for !done {
		// Check for cancellation
		select {
		case <-ctx.Done():
			log.Printf("Worker %d stopping due to context cancellation", workerID)
			return ctx.Err()
		default:
		}

		var vertex *VertexData

		if isDurationBased {
			// Duration-based: cycle through vertices
			actualIndex := processingIndices[vertexIndex%len(processingIndices)]
			if preGenerate {
				vertex = vertices[actualIndex]
			} else {
				vertex = generateVertexForIndex(actualIndex)
			}
			vertexIndex++
		} else {
			// Count-based: process assigned range
			verticesPerWorker := int(math.Ceil(float64(totalVertices) / float64(VUS)))
			startIdx := workerID * verticesPerWorker
			endIdx := (workerID + 1) * verticesPerWorker
			if endIdx > totalVertices {
				endIdx = totalVertices
			}

			if vertexIndex >= (endIdx - startIdx) {
				done = true
				break
			}

			actualIndex := processingIndices[startIdx+vertexIndex]
			if preGenerate {
				vertex = vertices[actualIndex]
			} else {
				vertex = generateVertexForIndex(actualIndex)
			}
			vertexIndex++
		}

		// Build and execute mutation
		mutation := buildVertexMutation(vertex)

		// Execute insert
		insertStart := time.Now()
		_, err := client.Apply(ctx, []*spanner.Mutation{mutation}, spanner.ApplyAtLeastOnce())
		insertDuration := time.Since(insertStart)

		// Record metrics - both atomic counters and detailed latency
		atomic.AddUint64(&totalInserts, 1)
		atomic.AddUint64(&currentTPS, 1)
		metricsCollector.AddDuration(workerID, insertDuration)

		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("Worker %d insert failed for UID %d: %s", workerID, vertex.UID, err.Error())
			workerErrorCount++
			atomic.AddUint64(&errorInserts, 1)
			metricsCollector.AddError(1)
		} else {
			workerSuccessCount++
			atomic.AddUint64(&successInserts, 1)
			metricsCollector.AddSuccess(1)
		}
		// Log progress every 100 inserts
		if workerSuccessCount%100 == 0 && workerSuccessCount > 0 {
			log.Printf("Worker %d processed %d vertices (success: %d, errors: %d, avg latency: %.2fms)",
				workerID, workerSuccessCount+workerErrorCount, workerSuccessCount, workerErrorCount,
				float64(insertDuration.Nanoseconds())/1000000)
		}

		// Small delay to prevent overwhelming the system
		if !isDurationBased {
			time.Sleep(1 * time.Millisecond)
		}
	}

	log.Printf("Worker %d completed: %d success, %d errors", workerID, workerSuccessCount, workerErrorCount)
	return nil
}

// writeEdgeWorkerWithMetrics is a worker function for writing edges with detailed metrics collection
func writeEdgeWorkerWithMetrics(ctx context.Context, client *spanner.Client, workerID int, playerProcessingIndices []int, startZoneID, endZoneID int, batchNum int, isDurationBased bool, metricsCollector *metrics.ConcurrentMetrics) error {
	log.Printf("Edge Worker %d started", workerID)

	// Configure max commit delay for throughput optimization
	var applyOpts []spanner.ApplyOption
	if MaxCommitDelayMs > 0 {
		maxDelay := time.Duration(MaxCommitDelayMs) * time.Millisecond
		commitOpts := spanner.CommitOptions{MaxCommitDelay: &maxDelay}
		applyOpts = []spanner.ApplyOption{
			spanner.ApplyCommitOptions(commitOpts),
		}
		log.Printf("Edge Worker %d using max commit delay: %dms for throughput optimization", workerID, MaxCommitDelayMs)
	}
	applyOpts = append(applyOpts, spanner.ApplyAtLeastOnce())

	// Calculate total zones and players
	totalZones := endZoneID - startZoneID
	totalPlayers := int64(totalZones * RECORDS_PER_ZONE)
	playersPerWorker := int(math.Ceil(float64(totalPlayers) / float64(VUS)))

	var stopTimer *time.Timer
	var done bool = false

	if isDurationBased {
		stopTimer = time.NewTimer(time.Duration(TEST_DURATION_SEC) * time.Second)
		defer func() {
			if stopTimer != nil {
				stopTimer.Stop()
			}
		}()

		go func() {
			select {
			case <-stopTimer.C:
				log.Printf("Edge Worker %d completed due to timer", workerID)
				done = true
			case <-ctx.Done():
				log.Printf("Edge Worker %d completed due to context cancellation", workerID)
				done = true
			}
		}()
	} else {
		// Count-based mode: calculate range for this worker
		workerStartIndex := workerID * playersPerWorker
		workerEndIndex := (workerID + 1) * playersPerWorker
		if workerEndIndex > int(totalPlayers) {
			workerEndIndex = int(totalPlayers)
		}
		log.Printf("Edge Worker %d processing players %d to %d", workerID, workerStartIndex, workerEndIndex-1)
	}

	workerSuccessCount := 0
	workerErrorCount := 0
	var playerIndex int = 0

	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

	for !done {
		// Check for cancellation
		select {
		case <-ctx.Done():
			log.Printf("Edge Worker %d stopping due to context cancellation", workerID)
			return ctx.Err()
		default:
		}

		var actualPlayerIndex int
		var playerUID int64

		if isDurationBased {
			// Duration-based: cycle through players
			actualPlayerIndex = playerProcessingIndices[playerIndex%len(playerProcessingIndices)]
			playerIndex++
		} else {
			// Count-based: process assigned range
			workerStartIndex := workerID * playersPerWorker
			workerEndIndex := (workerID + 1) * playersPerWorker
			if workerEndIndex > int(totalPlayers) {
				workerEndIndex = int(totalPlayers)
			}

			if playerIndex >= (workerEndIndex - workerStartIndex) {
				done = true
				break
			}

			actualPlayerIndex = playerProcessingIndices[workerStartIndex+playerIndex]
			playerIndex++
		}

		// Convert actualPlayerIndex to corresponding UID
		zoneOffset := actualPlayerIndex / RECORDS_PER_ZONE
		idInZone := actualPlayerIndex%RECORDS_PER_ZONE + 1
		currentZoneID := startZoneID + zoneOffset
		playerUID = (int64(currentZoneID) << 40) | int64(idInZone)

		// Create 5 types of relationships for this player
		relationTypes := []string{"Rel1", "Rel2", "Rel3", "Rel4", "Rel5"}

		// Move batch logic outside relationship loop to respect batchNum
		var mutations []*spanner.Mutation

		for _, relType := range relationTypes {
			// Track used targets for this relationship type to prevent duplicates
			usedTargets := make(map[int64]bool)

			edgesCreated := 0
			for edgesCreated < EDGES_PER_RELATION {
				var targetUID int64
				maxRetries := 100 // Prevent infinite loops
				found := false

				// Try to find a unique target UID
				for retry := 0; retry < maxRetries; retry++ {
					targetZoneID := ZONE_START + rng.Intn(ZONES_TOTAL)
					targetID := 1 + rng.Intn(RECORDS_PER_ZONE)
					targetUID = (int64(targetZoneID) << 40) | int64(targetID)

					// Check for self-loops and duplicates
					if targetUID != playerUID && !usedTargets[targetUID] {
						usedTargets[targetUID] = true
						found = true
						break
					}
				}

				if !found {
					// If we can't find a unique target after maxRetries, log warning and continue
					log.Printf("Edge Worker %d: Could not find unique target for player %d (%s) after %d retries, created %d/%d edges",
						workerID, playerUID, relType, maxRetries, edgesCreated, EDGES_PER_RELATION)
					break
				}

				// Generate edge attributes attr101-attr110
				edgeAttrs := make([]int64, 10)
				for j := 0; j < 10; j++ {
					edgeAttrs[j] = rng.Int63n(10000)
				}

				// Build mutation for this edge
				mutation := buildEdgeMutation(relType, playerUID, targetUID, edgeAttrs)
				mutations = append(mutations, mutation)
				edgesCreated++

				// Execute batch when reaching batchNum (respects configured batch size)
				if len(mutations) >= batchNum {
					insertStart := time.Now()
					_, err := client.Apply(ctx, mutations, applyOpts...)
					insertDuration := time.Since(insertStart)

					// Record metrics - both atomic counters and detailed latency
					atomic.AddUint64(&totalInserts, uint64(len(mutations)))
					atomic.AddUint64(&currentTPS, uint64(len(mutations)))
					metricsCollector.AddDuration(workerID, insertDuration)

					if err != nil {
						if ctx.Err() != nil {
							return ctx.Err()
						}
						log.Printf("Edge Worker %d batch insert failed for player %d, batch size %d: %s",
							workerID, playerUID, len(mutations), err.Error())
						workerErrorCount += len(mutations)
						atomic.AddUint64(&errorInserts, uint64(len(mutations)))
						metricsCollector.AddError(int64(len(mutations)))
					} else {
						workerSuccessCount += len(mutations)
						atomic.AddUint64(&successInserts, uint64(len(mutations)))
						metricsCollector.AddSuccess(int64(len(mutations)))
					}

					// Reset mutations slice for next batch
					mutations = []*spanner.Mutation{}
				}
			}
		}

		// Execute any remaining mutations after all relationships for this player
		if len(mutations) > 0 {
			insertStart := time.Now()
			_, err := client.Apply(ctx, mutations, applyOpts...)
			insertDuration := time.Since(insertStart)

			// Record metrics for the final batch
			atomic.AddUint64(&totalInserts, uint64(len(mutations)))
			atomic.AddUint64(&currentTPS, uint64(len(mutations)))
			metricsCollector.AddDuration(workerID, insertDuration)

			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				log.Printf("Edge Worker %d final batch insert failed for player %d, batch size %d: %s",
					workerID, playerUID, len(mutations), err.Error())
				workerErrorCount += len(mutations)
				atomic.AddUint64(&errorInserts, uint64(len(mutations)))
				metricsCollector.AddError(int64(len(mutations)))
			} else {
				workerSuccessCount += len(mutations)
				atomic.AddUint64(&successInserts, uint64(len(mutations)))
				metricsCollector.AddSuccess(int64(len(mutations)))
			}
		}

		// Log progress every 100 players
		if workerSuccessCount > 0 && workerSuccessCount%500 == 0 {
			log.Printf("Edge Worker %d processed player %d (UID: %d), success: %d, errors: %d",
				workerID, playerIndex, playerUID, workerSuccessCount, workerErrorCount)
		}

		// Small delay to prevent overwhelming the system
		if !isDurationBased {
			time.Sleep(1 * time.Millisecond)
		}
	}

	log.Printf("Edge Worker %d completed: %d success, %d errors", workerID, workerSuccessCount, workerErrorCount)
	return nil
}

// spannerReadRelationTest performs improved edge relation read testing using errgroup and duration-based testing.
// It scans every vertex within the configured zone range, reads up to 300 destination UIDs connected via
// Rel1 / Rel4 / Rel5 edges that satisfy attr101>1000, attr102>2000, attr103>4000 and records latency metrics.
func spannerReadRelationTest(client *spanner.Client, dbPath string) {
	log.Println("Starting improved Spanner relation read test…")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handleInterrupt(cancel)
	startTPSMonitor(ctx)

	startTime := time.Now()

	// Initialize detailed metrics collector
	metricsCollector := metrics.NewConcurrentMetrics(VUS)

	// Reset atomic counters for read metrics
	atomic.StoreUint64(&totalInserts, 0)
	atomic.StoreUint64(&successInserts, 0)
	atomic.StoreUint64(&errorInserts, 0)
	atomic.StoreUint64(&currentTPS, 0)

	totalVertices := ZONES_TOTAL * RECORDS_PER_ZONE

	// Determine test mode
	isDurationBased := TEST_DURATION_SEC > 0
	if isDurationBased {
		log.Printf("Running duration-based relation read test: %d seconds with %d workers", TEST_DURATION_SEC, VUS)
	} else {
		log.Printf("Running count-based relation read test: %d vertices with %d workers", totalVertices, VUS)
	}

	log.Printf("Starting %d read workers with shared client, dbpath=%s", VUS, dbPath)
	log.Println("Press Ctrl+C at any time to stop gracefully...")
	countdownOrExit("开始读取关系", 5)

	group, grpCtx := errgroup.WithContext(ctx)

	// Launch relation read workers
	for workerID := 0; workerID < VUS; workerID++ {
		workerID := workerID // capture loop variable
		group.Go(func() error {
			return readRelationWorkerWithMetrics(grpCtx, client, workerID, totalVertices, isDurationBased, metricsCollector)
		})
	}

	// Wait for all workers to complete
	if err := group.Wait(); err != nil {
		log.Printf("Relation read worker error: %v", err)
	}

	totalDuration := time.Since(startTime)
	final_total := atomic.LoadUint64(&totalInserts)
	final_success := atomic.LoadUint64(&successInserts)
	final_errors := atomic.LoadUint64(&errorInserts)

	// Get combined metrics for detailed latency breakdown
	combinedMetrics := metricsCollector.CombinedStats()
	metricsSuccess := metricsCollector.GetSuccessCount()
	metricsErrors := metricsCollector.GetErrorCount()

	if ctx.Err() != nil {
		log.Println("Improved relation read test interrupted by user:")
	} else {
		log.Println("Improved relation read test completed:")
	}
	log.Printf("  Total duration: %v", totalDuration)
	log.Printf("  Total attempts: %d", final_total)
	log.Printf("  Expected queries: %d", totalVertices)
	log.Printf("  Successful queries: %d (metrics: %d)", final_success, metricsSuccess)
	log.Printf("  Failed queries: %d (metrics: %d)", final_errors, metricsErrors)
	if final_total > 0 {
		log.Printf("  Success rate: %.2f%%", float64(final_success)*100/float64(final_total))
	}
	log.Printf("  Throughput: %.2f queries/sec", float64(final_success)/totalDuration.Seconds())
	log.Printf("  Latency metrics: %s", combinedMetrics.String())
}

// readRelationWorkerWithMetrics is a worker function for reading relations with detailed metrics collection
func readRelationWorkerWithMetrics(ctx context.Context, client *spanner.Client, workerID int, totalVertices int, isDurationBased bool, metricsCollector *metrics.ConcurrentMetrics) error {
	log.Printf("Relation Read Worker %d started", workerID)

	var stopTimer *time.Timer
	var done bool = false

	if isDurationBased {
		stopTimer = time.NewTimer(time.Duration(TEST_DURATION_SEC) * time.Second)
		defer func() {
			if stopTimer != nil {
				stopTimer.Stop()
			}
		}()

		go func() {
			select {
			case <-stopTimer.C:
				log.Printf("Relation Read Worker %d completed due to timer", workerID)
				done = true
			case <-ctx.Done():
				log.Printf("Relation Read Worker %d completed due to context cancellation", workerID)
				done = true
			}
		}()
	} else {
		// Count-based mode: calculate range for this worker
		verticesPerWorker := int(math.Ceil(float64(totalVertices) / float64(VUS)))
		startIdx := workerID * verticesPerWorker
		endIdx := (workerID + 1) * verticesPerWorker
		if endIdx > totalVertices {
			endIdx = totalVertices
		}
		log.Printf("Relation Read Worker %d processing vertices %d to %d", workerID, startIdx, endIdx-1)
	}

	workerSuccessCount := 0
	workerErrorCount := 0
	var vertexIndex int = 0

	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

	const baseSQL = `
		GRAPH %s
		MATCH (a:User {uid:@uid})-[e:Rel1|Rel4|Rel5]->(b:User)
		WHERE e.attr101 > @a101 AND e.attr102 > @a102 AND e.attr103 > @a103
		RETURN a.uid AS a_uid, a.attr1 AS a_attr1, a.attr2 AS a_attr2, a.attr3 AS a_attr3, a.attr4 AS a_attr4, a.attr5 AS a_attr5, a.attr6 AS a_attr6, a.attr7 AS a_attr7, a.attr8 AS a_attr8, a.attr9 AS a_attr9, a.attr10 AS a_attr10,
		       a.attr11 AS a_attr11, a.attr12 AS a_attr12, a.attr13 AS a_attr13, a.attr14 AS a_attr14, a.attr15 AS a_attr15, a.attr16 AS a_attr16, a.attr17 AS a_attr17, a.attr18 AS a_attr18, a.attr19 AS a_attr19, a.attr20 AS a_attr20,
		       a.attr21 AS a_attr21, a.attr22 AS a_attr22, a.attr23 AS a_attr23, a.attr24 AS a_attr24, a.attr25 AS a_attr25, a.attr26 AS a_attr26, a.attr27 AS a_attr27, a.attr28 AS a_attr28, a.attr29 AS a_attr29, a.attr30 AS a_attr30,
		       a.attr31 AS a_attr31, a.attr32 AS a_attr32, a.attr33 AS a_attr33, a.attr34 AS a_attr34, a.attr35 AS a_attr35, a.attr36 AS a_attr36, a.attr37 AS a_attr37, a.attr38 AS a_attr38, a.attr39 AS a_attr39, a.attr40 AS a_attr40,
		       a.attr41 AS a_attr41, a.attr42 AS a_attr42, a.attr43 AS a_attr43, a.attr44 AS a_attr44, a.attr45 AS a_attr45, a.attr46 AS a_attr46, a.attr47 AS a_attr47, a.attr48 AS a_attr48, a.attr49 AS a_attr49, a.attr50 AS a_attr50,
		       a.attr51 AS a_attr51, a.attr52 AS a_attr52, a.attr53 AS a_attr53, a.attr54 AS a_attr54, a.attr55 AS a_attr55, a.attr56 AS a_attr56, a.attr57 AS a_attr57, a.attr58 AS a_attr58, a.attr59 AS a_attr59, a.attr60 AS a_attr60,
		       a.attr61 AS a_attr61, a.attr62 AS a_attr62, a.attr63 AS a_attr63, a.attr64 AS a_attr64, a.attr65 AS a_attr65, a.attr66 AS a_attr66, a.attr67 AS a_attr67, a.attr68 AS a_attr68, a.attr69 AS a_attr69, a.attr70 AS a_attr70,
		       a.attr71 AS a_attr71, a.attr72 AS a_attr72, a.attr73 AS a_attr73, a.attr74 AS a_attr74, a.attr75 AS a_attr75, a.attr76 AS a_attr76, a.attr77 AS a_attr77, a.attr78 AS a_attr78, a.attr79 AS a_attr79, a.attr80 AS a_attr80,
		       a.attr81 AS a_attr81, a.attr82 AS a_attr82, a.attr83 AS a_attr83, a.attr84 AS a_attr84, a.attr85 AS a_attr85, a.attr86 AS a_attr86, a.attr87 AS a_attr87, a.attr88 AS a_attr88, a.attr89 AS a_attr89, a.attr90 AS a_attr90,
		       a.attr91 AS a_attr91, a.attr92 AS a_attr92, a.attr93 AS a_attr93, a.attr94 AS a_attr94, a.attr95 AS a_attr95, a.attr96 AS a_attr96, a.attr97 AS a_attr97, a.attr98 AS a_attr98, a.attr99 AS a_attr99, a.attr100 AS a_attr100,
		       b.uid AS b_uid, b.attr1 AS b_attr1, b.attr2 AS b_attr2, b.attr3 AS b_attr3, b.attr4 AS b_attr4, b.attr5 AS b_attr5, b.attr6 AS b_attr6, b.attr7 AS b_attr7, b.attr8 AS b_attr8, b.attr9 AS b_attr9, b.attr10 AS b_attr10,
		       b.attr11 AS b_attr11, b.attr12 AS b_attr12, b.attr13 AS b_attr13, b.attr14 AS b_attr14, b.attr15 AS b_attr15, b.attr16 AS b_attr16, b.attr17 AS b_attr17, b.attr18 AS b_attr18, b.attr19 AS b_attr19, b.attr20 AS b_attr20,
		       b.attr21 AS b_attr21, b.attr22 AS b_attr22, b.attr23 AS b_attr23, b.attr24 AS b_attr24, b.attr25 AS b_attr25, b.attr26 AS b_attr26, b.attr27 AS b_attr27, b.attr28 AS b_attr28, b.attr29 AS b_attr29, b.attr30 AS b_attr30,
		       b.attr31 AS b_attr31, b.attr32 AS b_attr32, b.attr33 AS b_attr33, b.attr34 AS b_attr34, b.attr35 AS b_attr35, b.attr36 AS b_attr36, b.attr37 AS b_attr37, b.attr38 AS b_attr38, b.attr39 AS b_attr39, b.attr40 AS b_attr40,
		       b.attr41 AS b_attr41, b.attr42 AS b_attr42, b.attr43 AS b_attr43, b.attr44 AS b_attr44, b.attr45 AS b_attr45, b.attr46 AS b_attr46, b.attr47 AS b_attr47, b.attr48 AS b_attr48, b.attr49 AS b_attr49, b.attr50 AS b_attr50,
		       b.attr51 AS b_attr51, b.attr52 AS b_attr52, b.attr53 AS b_attr53, b.attr54 AS b_attr54, b.attr55 AS b_attr55, b.attr56 AS b_attr56, b.attr57 AS b_attr57, b.attr58 AS b_attr58, b.attr59 AS b_attr59, b.attr60 AS b_attr60,
		       b.attr61 AS b_attr61, b.attr62 AS b_attr62, b.attr63 AS b_attr63, b.attr64 AS b_attr64, b.attr65 AS b_attr65, b.attr66 AS b_attr66, b.attr67 AS b_attr67, b.attr68 AS b_attr68, b.attr69 AS b_attr69, b.attr70 AS b_attr70,
		       b.attr71 AS b_attr71, b.attr72 AS b_attr72, b.attr73 AS b_attr73, b.attr74 AS b_attr74, b.attr75 AS b_attr75, b.attr76 AS b_attr76, b.attr77 AS b_attr77, b.attr78 AS b_attr78, b.attr79 AS b_attr79, b.attr80 AS b_attr80,
		       b.attr81 AS b_attr81, b.attr82 AS b_attr82, b.attr83 AS b_attr83, b.attr84 AS b_attr84, b.attr85 AS b_attr85, b.attr86 AS b_attr86, b.attr87 AS b_attr87, b.attr88 AS b_attr88, b.attr89 AS b_attr89, b.attr90 AS b_attr90,
		       b.attr91 AS b_attr91, b.attr92 AS b_attr92, b.attr93 AS b_attr93, b.attr94 AS b_attr94, b.attr95 AS b_attr95, b.attr96 AS b_attr96, b.attr97 AS b_attr97, b.attr98 AS b_attr98, b.attr99 AS b_attr99, b.attr100 AS b_attr100
		LIMIT 300`

	for !done {
		// Check for cancellation
		select {
		case <-ctx.Done():
			log.Printf("Relation Read Worker %d stopping due to context cancellation", workerID)
			return ctx.Err()
		default:
		}

		var idx int
		var uid int64

		if isDurationBased {
			// Duration-based: cycle through vertices
			idx = vertexIndex % totalVertices
			vertexIndex++
		} else {
			// Count-based: process assigned range
			verticesPerWorker := int(math.Ceil(float64(totalVertices) / float64(VUS)))
			startIdx := workerID * verticesPerWorker
			endIdx := (workerID + 1) * verticesPerWorker
			if endIdx > totalVertices {
				endIdx = totalVertices
			}

			if vertexIndex >= (endIdx - startIdx) {
				done = true
				break
			}

			idx = startIdx + vertexIndex
			vertexIndex++
		}

		// Derive UID from idx
		zoneOffset := idx / RECORDS_PER_ZONE
		idInZone := idx%RECORDS_PER_ZONE + 1
		zoneID := ZONE_START + zoneOffset
		uid = (int64(zoneID) << 40) | int64(idInZone)

		stmt := spanner.Statement{
			SQL: fmt.Sprintf(baseSQL, GRAPH_NAME), // stable text!
			Params: map[string]interface{}{ // literals become params
				"uid":  uid,
				"a101": rng.Intn(10000),
				"a102": rng.Intn(10000),
				"a103": rng.Intn(10000),
			},
		}

		var ro *spanner.ReadOnlyTransaction
		queryStart := time.Now()
		// Create a new single-use read-only transaction for each query
		// Create a new single-use read-only transaction for each query
		if UseStaleReads {
			// Configure stale read based on settings
			if StaleReadMode == "max" {
				// Maximum staleness - read data that's at most N milliseconds old
				ro = client.Single().WithTimestampBound(
					spanner.MaxStaleness(time.Duration(StalenessMs) * time.Millisecond))
			} else if StaleReadMode == "exact" {
				// Exact staleness - read data that's exactly N milliseconds old
				ro = client.Single().WithTimestampBound(
					spanner.ExactStaleness(time.Duration(StalenessMs) * time.Millisecond))
			}
		} else {
			// Use strong consistency (default)
			ro = client.Single()
		}
		iterRows := ro.Query(ctx, stmt)

		// Consume rows and capture errors
		rowCnt := 0
		success := true
		for {
			_, err := iterRows.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.Printf("Relation Read Worker %d query failed for uid %d: %v", workerID, uid, err)
				success = false
				break
			}
			// printSpannerRow(row)
			rowCnt++
		}
		iterRows.Stop()
		ro.Close() // Close the transaction after each query

		// Measure the complete query duration including result consumption
		queryDuration := time.Since(queryStart)

		// Record metrics - both atomic counters and detailed latency
		atomic.AddUint64(&totalInserts, 1)
		atomic.AddUint64(&currentTPS, 1)
		metricsCollector.AddDuration(workerID, queryDuration)

		if success {
			workerSuccessCount++
			atomic.AddUint64(&successInserts, 1)
			metricsCollector.AddSuccess(1)
		} else {
			workerErrorCount++
			atomic.AddUint64(&errorInserts, 1)
			metricsCollector.AddError(1)
		}

		// Log progress every 500 queries
		if workerSuccessCount > 0 && workerSuccessCount%500 == 0 {
			log.Printf("Relation Read Worker %d, iter %d, uid %d, query time: %v, rows: %d", workerID, vertexIndex, uid, queryDuration, rowCnt)
		}

		// Small delay to prevent overwhelming the system
		if !isDurationBased {
			time.Sleep(1 * time.Millisecond)
		}
	}

	log.Printf("Relation Read Worker %d completed: %d success, %d errors", workerID, workerSuccessCount, workerErrorCount)
	return nil
}

func spannerReadVertexTest(client *spanner.Client) {
	log.Println("Starting improved Spanner read vertex test...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handleInterrupt(cancel)
	startTPSMonitor(ctx)

	startTime := time.Now()

	// Initialize detailed metrics collector
	metricsCollector := metrics.NewConcurrentMetrics(VUS)

	// Reset atomic counters for read metrics
	atomic.StoreUint64(&totalInserts, 0)
	atomic.StoreUint64(&successInserts, 0)
	atomic.StoreUint64(&errorInserts, 0)
	atomic.StoreUint64(&currentTPS, 0)

	// Total queries to execute
	totalQueries := 1000000

	// Determine test mode
	isDurationBased := TEST_DURATION_SEC > 0
	if isDurationBased {
		log.Printf("Running duration-based vertex read test: %d seconds with %d workers", TEST_DURATION_SEC, VUS)
	} else {
		log.Printf("Running count-based vertex read test: %d queries with %d workers", totalQueries, VUS)
	}

	log.Printf("Starting %d vertex read workers...", VUS)
	log.Println("Press Ctrl+C at any time to stop gracefully...")
	countdownOrExit("开始读顶点", 5)

	group, grpCtx := errgroup.WithContext(ctx)

	// Launch vertex read workers
	for workerID := 0; workerID < VUS; workerID++ {
		workerID := workerID // capture loop variable
		group.Go(func() error {
			return readVertexWorkerWithMetrics(grpCtx, client, workerID, totalQueries, isDurationBased, metricsCollector)
		})
	}

	// Wait for all workers to complete
	if err := group.Wait(); err != nil {
		log.Printf("Vertex read worker error: %v", err)
	}

	totalDuration := time.Since(startTime)
	final_total := atomic.LoadUint64(&totalInserts)
	final_success := atomic.LoadUint64(&successInserts)
	final_errors := atomic.LoadUint64(&errorInserts)

	// Get combined metrics for detailed latency breakdown
	combinedMetrics := metricsCollector.CombinedStats()
	metricsSuccess := metricsCollector.GetSuccessCount()
	metricsErrors := metricsCollector.GetErrorCount()

	if ctx.Err() != nil {
		log.Println("Improved vertex read test interrupted by user:")
	} else {
		log.Println("Improved vertex read test completed:")
	}
	log.Printf("  Total duration: %v", totalDuration)
	log.Printf("  Total attempts: %d", final_total)
	log.Printf("  Expected queries: %d", totalQueries)
	log.Printf("  Successful queries: %d (metrics: %d)", final_success, metricsSuccess)
	log.Printf("  Failed queries: %d (metrics: %d)", final_errors, metricsErrors)
	if final_total > 0 {
		log.Printf("  Success rate: %.2f%%", float64(final_success)*100/float64(final_total))
	}
	log.Printf("  Throughput: %.2f queries/sec", float64(final_success)/totalDuration.Seconds())
	log.Printf("  Latency metrics: %s", combinedMetrics.String())
}

// readVertexWorkerWithMetrics is a worker function for reading vertices with detailed metrics collection
func readVertexWorkerWithMetrics(ctx context.Context, client *spanner.Client, workerID int, totalQueries int, isDurationBased bool, metricsCollector *metrics.ConcurrentMetrics) error {
	log.Printf("Vertex Read Worker %d started", workerID)

	var stopTimer *time.Timer
	var done bool = false

	if isDurationBased {
		stopTimer = time.NewTimer(time.Duration(TEST_DURATION_SEC) * time.Second)
		defer func() {
			if stopTimer != nil {
				stopTimer.Stop()
			}
		}()

		go func() {
			select {
			case <-stopTimer.C:
				log.Printf("Vertex Read Worker %d completed due to timer", workerID)
				done = true
			case <-ctx.Done():
				log.Printf("Vertex Read Worker %d completed due to context cancellation", workerID)
				done = true
			}
		}()
	} else {
		// Count-based mode: calculate range for this worker
		queriesPerWorker := int(math.Ceil(float64(totalQueries) / float64(VUS)))
		startIdx := workerID * queriesPerWorker
		endIdx := (workerID + 1) * queriesPerWorker
		if endIdx > totalQueries {
			endIdx = totalQueries
		}
		log.Printf("Vertex Read Worker %d processing queries %d to %d", workerID, startIdx, endIdx-1)
	}

	workerSuccessCount := 0
	workerErrorCount := 0
	var queryIndex int = 0

	// Create random generator for this worker
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

	for !done {
		// Check for cancellation
		select {
		case <-ctx.Done():
			log.Printf("Vertex Read Worker %d stopping due to context cancellation", workerID)
			return ctx.Err()
		default:
		}

		if !isDurationBased {
			// Count-based: process assigned range
			queriesPerWorker := int(math.Ceil(float64(totalQueries) / float64(VUS)))
			startIdx := workerID * queriesPerWorker
			endIdx := (workerID + 1) * queriesPerWorker
			if endIdx > totalQueries {
				endIdx = totalQueries
			}

			if queryIndex >= (endIdx - startIdx) {
				done = true
				break
			}
		}

		// Generate random values for WHERE clause [0,9999)
		attr11Value := rng.Intn(9999)
		attr12Value := rng.Intn(9999)
		attr13Value := rng.Intn(9999)
		var sql string
		//sql = `SELECT * FROM Users
		//		  WHERE attr11 = @attr11 AND attr12 > @attr12 AND attr13 > @attr13
		//		  LIMIT 300`
		//sql = `SELECT uid FROM Users
		//		  WHERE attr11 = @attr11 AND attr12 > @attr12 AND attr13 > @attr13
		//		  LIMIT 300`
		sql = `Select * from Users where uid in (SELECT uid FROM Users
				  WHERE attr11 = @attr11 AND attr12 > @attr12 AND attr13 > @attr13
				  LIMIT 300)`
		// Build parameterized query
		stmt := spanner.Statement{
			SQL: sql,
			Params: map[string]interface{}{
				"attr11": int64(attr11Value),
				"attr12": int64(attr12Value),
				"attr13": int64(attr13Value),
			},
		}
		var ro *spanner.ReadOnlyTransaction

		queryStart := time.Now()
		// Create a new single-use read-only transaction for each query
		if UseStaleReads {
			// Configure stale read based on settings
			if StaleReadMode == "max" {
				// Maximum staleness - read data that's at most N milliseconds old
				ro = client.Single().WithTimestampBound(
					spanner.MaxStaleness(time.Duration(StalenessMs) * time.Millisecond))
			} else if StaleReadMode == "exact" {
				// Exact staleness - read data that's exactly N milliseconds old
				ro = client.Single().WithTimestampBound(
					spanner.ExactStaleness(time.Duration(StalenessMs) * time.Millisecond))
			}
		} else {
			// Use strong consistency (default)
			ro = client.Single()
		}
		iterRows := ro.Query(ctx, stmt)

		// Count rows and consume results
		rowCount := 0
		success := true
		for {
			_, err := iterRows.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.Printf("Vertex Read Worker %d query failed: %v", workerID, err)
				success = false
				break
			}
			rowCount++
		}
		iterRows.Stop()
		ro.Close() // Close the transaction after each query

		queryDuration := time.Since(queryStart)

		// Record metrics - both atomic counters and detailed latency
		atomic.AddUint64(&totalInserts, 1)
		atomic.AddUint64(&currentTPS, 1)
		metricsCollector.AddDuration(workerID, queryDuration)

		if success {
			workerSuccessCount++
			atomic.AddUint64(&successInserts, 1)
			metricsCollector.AddSuccess(1)
		} else {
			workerErrorCount++
			atomic.AddUint64(&errorInserts, 1)
			metricsCollector.AddError(1)
		}

		queryIndex++

		// Log progress every 100 queries
		if workerSuccessCount > 0 && workerSuccessCount%100 == 0 {
			log.Printf("Vertex Read Worker %d, iter %d, attr11=%d, attr12>%d, attr13>%d, query time: %v, rows: %d",
				workerID, queryIndex, attr11Value, attr12Value, attr13Value, queryDuration, rowCount)
		}

		// Small delay to prevent overwhelming the system
		if !isDurationBased {
			time.Sleep(1 * time.Millisecond)
		}
	}

	log.Printf("Vertex Read Worker %d completed: %d success, %d errors", workerID, workerSuccessCount, workerErrorCount)
	return nil
}

// AddGFELatencyUnaryInterceptor intercepts unary client requests and adds GFE latency to existing spans
func AddGFELatencyUnaryInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {

	var headers metadata.MD
	opts = append(opts, grpc.Header(&headers))

	// Execute the gRPC call
	err := invoker(ctx, method, req, reply, cc, opts...)

	// Get the current span from context (created by otelgrpc instrumentation)
	span := oteltrace.SpanFromContext(ctx)
	if span.IsRecording() {
		// Parse and add GFE latency attributes to the existing span
		if latencyType, gfeLatency, parseErr := parseT4T7Latency(headers); parseErr == nil {
			span.SetAttributes(
				attribute.String(latencyType+".latency", gfeLatency.String()),
				attribute.Int64(latencyType+".latency_us", gfeLatency.Microseconds()),
				attribute.Float64(latencyType+".latency_ms", float64(gfeLatency.Nanoseconds())/1e6),
			)
			//log.Printf("GFE latency for %s: %v", method, gfeLatency)
		} else {
			span.SetAttributes(attribute.String("gfe.latency_error", parseErr.Error()))
		}

		// Add additional useful attributes
		span.SetAttributes(
			attribute.String("component", "spanner-client"),
			attribute.String("interceptor", "gfe-latency"),
		)
	}

	return err
}

// AddGFELatencyStreamingInterceptor intercepts streaming requests and adds GFE latency to existing spans
func AddGFELatencyStreamingInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {

	var headers metadata.MD
	opts = append(opts, grpc.Header(&headers))

	cs, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		return cs, err
	}

	// Get the current span from context (created by otelgrpc instrumentation)
	span := oteltrace.SpanFromContext(ctx)

	// For streaming calls, parse latency when the stream completes
	go func() {
		<-ctx.Done()

		if span.IsRecording() {
			// Parse and add GFE latency attributes to the existing span
			if latencyType, gfeLatency, parseErr := parseT4T7Latency(headers); parseErr == nil {
				span.SetAttributes(
					attribute.String(latencyType+".latency", gfeLatency.String()),
					attribute.Int64(latencyType+".latency_us", gfeLatency.Microseconds()),
					attribute.Float64(latencyType+".latency_ms", float64(gfeLatency.Nanoseconds())/1e6),
				)
				log.Printf("GFE latency for %s: %v", method, gfeLatency)
			} else {
				span.SetAttributes(attribute.String("gfe.latency_error", parseErr.Error()))
			}

			// Add additional useful attributes
			span.SetAttributes(
				attribute.String("component", "spanner-client"),
				attribute.String("interceptor", "gfe-latency"),
			)
		}
	}()

	return cs, nil
}

func main() {
	cleanup := initOpenTelemetryTracer()
	defer cleanup()

	initFromEnv()

	// Define command line flags
	var testType string
	var startZone, endZone int
	var batchNum int
	var useStaleReads bool
	var staleReadMode string
	var stalenessMs int
	flag.StringVar(&testType, "test", "setup", "Test type to run: setup, setupindex, write-vertex, write-vertex-improved, delete-vertex-using-pdml, write-edge, relation, all")
	flag.IntVar(&startZone, "start-zone", ZONE_START, "Start zone ID for edge test")
	flag.IntVar(&endZone, "end-zone", ZONE_START+ZONES_TOTAL, "End zone ID for edge test")
	flag.IntVar(&batchNum, "batch-num", EDGES_PER_RELATION, "Number of edges per batch for edge write test")
	flag.BoolVar(&useStaleReads, "stale-reads", UseStaleReads, "Enable stale reads for read operations")
	flag.StringVar(&staleReadMode, "stale-mode", StaleReadMode, "Stale read mode: max or exact")
	flag.IntVar(&stalenessMs, "staleness-ms", StalenessMs, "Staleness time in milliseconds")
	flag.Parse()

	ctx := context.Background()
	// Update global variables from command line flags
	UseStaleReads = useStaleReads
	StaleReadMode = staleReadMode
	StalenessMs = stalenessMs
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)
	os.Setenv("GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS", "true")
	os.Setenv("GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS_FOR_RW", "true")
	os.Setenv("SPANNER_DISABLE_BUILTIN_METRICS", "true")
	os.Setenv("SPANNER_DISABLE_DIRECT_ACCESS_GRPC_BUILTIN_METRICS", "true")
	os.Setenv("SPANNER_DISABLE_AFE_SERVER_TIMING", "true")
	os.Setenv("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS", "true")
	// Enable comprehensive tracing for Spanner client operations

	clientOpts := []option.ClientOption{
		//option.WithCredentialsFile(credentialsFile),
		option.WithGRPCConnectionPool(16),
		//option.WithGRPCDialOption(grpc.WithUnaryInterceptor(AddGFELatencyUnaryInterceptor)),
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(64*1024*1024), // Larger buffers
			grpc.MaxCallSendMsgSize(64*1024*1024),
		)),
		option.WithGRPCDialOption(grpc.WithStreamInterceptor(AddGFELatencyStreamingInterceptor)),
	}
	client, err := spanner.NewClientWithConfig(ctx, dbPath, spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{MaxOpened: 1, MinOpened: 1},
	},
		clientOpts...,
	)
	if err != nil {
		log.Printf("Failed to create Spanner client: %v", err)
		log.Fatalf("Spanner client required for test type: %s", testType)
	}
	defer client.Close()
	for i := 0; i < 16; i++ {
		if err = client.Single().Query(ctx, spanner.NewStatement("SELECT * from Users limit 1")).Do(func(row *spanner.Row) error {
			return nil
		}); err != nil {
			log.Printf("Failed to connect to Spanner database %s: %v", dbPath, err)
			log.Fatalf("Spanner client required for test type: %s", testType)
		}
	}
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
	case "delete-vertex-using-pdml":
		log.Println("Running vertex deletion test using PDML...")
		if err := spannerDeleteVertexUsingPDML(ctx, client); err != nil {
			log.Fatalf("Failed to delete vertices using PDML: %v", err)
		} else {
			log.Println("Successfully deleted all vertices using PDML")
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

	case "write-vertex-improved":
		log.Println("Running improved vertex write test...")
		spannerWriteVertexTestImproved(client, PreGenerateVertexData)
	case "write-edge":
		log.Printf("Running edge write test for zones [%d, %d)...", startZone, endZone)
		spannerWriteEdgeTest(client, startZone, endZone, batchNum)
	case "read-relation":
		log.Println("Running relation read test...")
		spannerReadRelationTest(client, dbPath)

	case "read-vertex":
		log.Println("Running vertex read test...")
		spannerReadVertexTest(client)
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
		log.Println("Available test types: trace-test, setup, setupindex, write-vertex, write-vertex-improved, delete-vertex-using-pdml, write-edge, relation, all")
		os.Exit(1)
	}

	log.Println("Benchmark completed")
}
