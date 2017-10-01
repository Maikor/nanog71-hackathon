/*
1. Configures a Streaming Telemetry subscription using an OpenConfig model template.
2. Subscribes to a Telemetry stream to learn interfaces stats.
3. Send a summary JSON output to a Kafka BUS.

Libraries:
	xrgrpc -> https://nleiva.github.io/xrgrpc/
	ygot -> https://github.com/openconfig/ygot/
*/
package main

import (
	"bufio"
	"context"
	"encoding/csv"
	j "encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"

	kafka "github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	octele "github.com/nleiva/nanog71/pkg/telemetry"
	xr "github.com/nleiva/xrgrpc"
	"github.com/nleiva/xrgrpc/proto/telemetry"
	"github.com/openconfig/ygot/ygot"
)

// Colors, just for fun.
const (
	blue   = "\x1b[34;1m"
	white  = "\x1b[0m"
	red    = "\x1b[31;1m"
	green  = "\x1b[32;1m"
	yellow = "\x1b[33;1m"
)

// Ifaces contains router interface info
type Ifaces struct {
	List []Iface `json:"interfaces"`
}

// Iface contains router interface info
type Iface struct {
	Name      string `json:"interface-name"`
	RcvdBytes string `json:"received-total-byte"`
	TranBytes string `json:"total-bytes-transmitted"`
}

func main() {
	var id int64
	ip := flag.String("ip", "10.1.3.20", "IP address")
	port := flag.String("port", "57777", "IP address")
	flag.Parse()

	////////////////////////////////////////////////////////////
	// Setting up Kafka Producer
	////////////////////////////////////////////////////////////
	config := kafka.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := kafka.NewAsyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		panic(err)
	}

	var (
		wg                          sync.WaitGroup
		enqueued, successes, errors int
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range producer.Successes() {
			successes++
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range producer.Errors() {
			log.Println(err)
			errors++
		}
	}()

	////////////////////////////////////////////////////////////
	// Manually specify target parameters
	////////////////////////////////////////////////////////////
	router, err := xr.BuildRouter(
		xr.WithUsername("vagrant"),
		xr.WithPassword("vagrant"),
		xr.WithHost(*ip+":"+*port),
		xr.WithCert("ems.pem"),
		xr.WithTimeout(3600),
	)
	if err != nil {
		log.Fatalf("target parameters are incorrect: %s", err)
	}

	// Extract the IP address
	r, err := net.ResolveTCPAddr("tcp", router.Host)
	if err != nil {
		log.Fatalf("Incorrect IP address: %v", err)
	}

	// Connect to the router
	conn, ctx, err := xr.Connect(*router)
	if err != nil {
		log.Fatalf("could not setup a client connection to %s, %v", r.IP, err)
	}
	defer conn.Close()

	// Dealing with Cancellation
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	defer func() {
		signal.Stop(c)
		cancel()
	}()

	////////////////////////////////////////////////////////////
	// Generate the Telemetry config
	////////////////////////////////////////////////////////////
	subscriptionID := "OC-Interface"
	sensorGroupID := "OpenConfig"
	path := "openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters"
	var sint uint64 = 5000

	t := &octele.OpenconfigTelemetry_TelemetrySystem{}
	ygot.BuildEmptyTree(t)
	sg, err := t.SensorGroups.NewSensorGroup(sensorGroupID)
	if err != nil {
		log.Fatalf("Failed to generate %s: %v", sensorGroupID, err)
	}
	ygot.BuildEmptyTree(sg)
	sg.Config.SensorGroupId = &sensorGroupID

	ygot.BuildEmptyTree(sg)
	sp, err := sg.SensorPaths.NewSensorPath(path)
	if err != nil {
		log.Fatalf("Failed to generate %s: %v", path, err)
	}
	ygot.BuildEmptyTree(sp)
	sp.Config.Path = &path

	sb, err := t.Subscriptions.Persistent.NewSubscription(subscriptionID)
	if err != nil {
		log.Fatalf("Failed to generate %s: %v", subscriptionID, err)
	}
	ygot.BuildEmptyTree(sb)
	sb.Config.SubscriptionId = &subscriptionID
	spf, err := sb.SensorProfiles.NewSensorProfile(sensorGroupID)
	if err != nil {
		log.Fatalf("Failed to generate %s: %v", sensorGroupID, err)
	}
	ygot.BuildEmptyTree(spf)
	spf.SensorGroup = &sensorGroupID
	spf.Config.SensorGroup = ygot.String(sensorGroupID)
	spf.Config.SampleInterval = ygot.Uint64(sint)

	// EmitJSON will do this anyways, here just for demo purposes
	if err := t.Validate(); err != nil {
		log.Fatalf("telemetry config validation failed: %v", err)
	}
	json, err := ygot.EmitJSON(t, &ygot.EmitJSONConfig{
		Format: ygot.RFC7951,
		Indent: "  ",
		RFC7951Config: &ygot.RFC7951JSONConfig{
			AppendModuleName: true,
		},
	})
	if err != nil {
		log.Fatalf("JSON generation failed: %v", err)
	}

	////////////////////////////////////////////////////////////
	// Apply Telemetry Config
	////////////////////////////////////////////////////////////
	_, err = xr.MergeConfig(ctx, conn, json, id)
	if err != nil {
		// log.Fatalf("failed to config %s: %v\n", r.IP, err)
	} else {
		fmt.Printf("\n1)\n%sTelemetry%s config applied on %s (Request ID: %v)\n", blue, white, r.IP, id)
	}
	id++

	// Pause
	fmt.Print("Press 'Enter' to continue...")
	bufio.NewReader(os.Stdin).ReadBytes('\n')

	////////////////////////////////////////////////////////////
	// Subscribe to Telemetry Stream
	////////////////////////////////////////////////////////////
	// Encoding GPBKV
	var e int64 = 3
	id++
	ch, ech, err := xr.GetSubscription(ctx, conn, subscriptionID, id, e)
	if err != nil {
		log.Fatalf("could not setup Telemetry Subscription: %v\n", err)
	}

	// Dealing with Cancellation (Telemetry Subscription)
	go func() {
		select {
		case <-c:
			fmt.Printf("\nmanually cancelled the session to %v\n\n", r.IP)
			cancel()
			producer.AsyncClose() // Trigger a shutdown of the producer.
			return
		case <-ctx.Done():
			// Timeout: "context deadline exceeded"
			err = ctx.Err()
			producer.AsyncClose() // Trigger a shutdown of the producer.
			fmt.Printf("\ngRPC session timed out after %v seconds: %v\n\n", router.Timeout, err.Error())
			return
		case err = <-ech:
			// Session canceled: "context canceled"
			producer.AsyncClose() // Trigger a shutdown of the producer.
			fmt.Printf("\ngRPC session to %v failed: %v\n\n", r.IP, err.Error())
			return
		}
	}()
	fmt.Printf("\n2)\nReceiving %sTelemetry%s from %s ->\n\n", blue, white, r.IP)

	////////////////////////////////////////////////////////////
	// Process Telemetry messages
	////////////////////////////////////////////////////////////
	for tele := range ch {
		message := new(telemetry.Telemetry)
		err := proto.Unmarshal(tele, message)
		if err != nil {
			log.Fatalf("could not unmarshall the message: %v\n", err)
		}
		// Create a CVS "file" with the data
		data := ""
		// DEBUG
		// fmt.Printf("%v\n", message.GetDataGpbkv())
		exploreFields(message.GetDataGpbkv(), &data)

		// read all of the records in CSV in to an slice
		cdata, err := csv.NewReader(strings.NewReader(data)).ReadAll()
		if err != nil {
			log.Fatal(err)
		}
		ifs := new(Ifaces)

		for _, i := range cdata {
			ifs.List = append(ifs.List, Iface{i[0], i[1], i[2]})
		}
		ji, _ := j.MarshalIndent(ifs, "", "   ")
		log.Printf("Sending Message to Kafka BUS\n")
		messageK := &kafka.ProducerMessage{Topic: "monitor", Value: kafka.StringEncoder(string(ji))}
		producer.Input() <- messageK
		enqueued++
	}
	wg.Wait()

	log.Printf("Successfully produced: %d; errors: %d\n", successes, errors)
}

func exploreFields(f []*telemetry.TelemetryField, d *string) {
	for _, field := range f {
		switch field.GetFields() {
		case nil:
			decodeKV(field, d)
		default:
			exploreFields(field.GetFields(), d)
		}
	}
}

func decodeKV(f *telemetry.TelemetryField, d *string) {
	switch f.GetValueByType().(type) {
	case *telemetry.TelemetryField_StringValue:
		switch f.GetName() {
		case "interface-name", "name":
			*d = *d + fmt.Sprintf("%v,", f.GetStringValue())
		default:
		}
	case *telemetry.TelemetryField_Uint64Value:
		switch f.GetName() {
		case "received-total-bytes", "in-crc-errors":
			*d = *d + fmt.Sprintf("%v,", f.GetUint64Value())
		case "total-bytes-transmitted", "out-8021q-frames":
			*d = *d + fmt.Sprintf("%v\n", f.GetUint64Value())
		default:
		}
	default:
	}
}
