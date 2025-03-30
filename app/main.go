package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	for {
		// Accept connection
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
	defer conn.Close()

	// Read from client into buffer of max size 1024 Bytes
	buf := make([]byte, 1024)
	if _, err := conn.Read(buf); err != nil {
		fmt.Println("Error while reading the response.")
	}

	// Fetch request api key
	request_api_version := binary.BigEndian.Uint16(buf[4:6])
	error_code := []byte{0, 0}
	if request_api_version > 4 || request_api_version < 0 {
		error_code = []byte{0, 35}
	}

	// Fetch correlation id
	correlation_id := buf[8:12]

	// Create response
	response := buildResponse(correlation_id, error_code)

	// Send response
	_, err := conn.Write(response)
	if err != nil {
		fmt.Println("Error sending response:", err)
	}
}

func buildResponse(correlation_id []byte, error_code []byte) (response []byte) {
	// Response header
	message_size := make([]byte, 4)

	// API_VERSIONS entry
	apiKey := make([]byte, 2)
	binary.BigEndian.PutUint16(apiKey, 18) // API key 18 (APIVersions)
	minVersion := make([]byte, 2)
	binary.BigEndian.PutUint16(minVersion, 0)
	maxVersion := make([]byte, 2)
	binary.BigEndian.PutUint16(maxVersion, 4) // Must be at least 4

	// Construct response body
	responseBody := []byte{}
	responseBody = append(responseBody, error_code...)
	responseBody = append(responseBody, apiKey...)
	responseBody = append(responseBody, minVersion...)
	responseBody = append(responseBody, maxVersion...)

	// Calculate final message size
	binary.BigEndian.PutUint32(message_size, uint32(len(responseBody)+4)) // +4 for correlation id

	// Final response
	response = append(response, message_size...)
	response = append(response, correlation_id...)
	response = append(response, responseBody...)

	return
}
