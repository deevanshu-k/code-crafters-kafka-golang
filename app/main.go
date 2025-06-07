package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit
var PORT = "9092"
var HOST = "0.0.0.0"

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("> Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%s", HOST, PORT))
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

	msg, err := readMessage(conn)
	if err != nil {
		fmt.Println("> Error reading message: ", err.Error())
		return
	}

	response, err := buildResponse(msg)
	if err != nil {
		fmt.Println("> Error building response: ", err.Error())
		return
	}

	fmt.Printf("> Request: %x\n", msg)
	fmt.Printf("> Response: %x\n", response)

	if _, err := conn.Write(response); err != nil {
		fmt.Println("> Error writing response: ", err.Error())
		return
	}
	fmt.Println("> Response sent successfully")
}

// READS MESSAGES FROM THE CONNECTION
// - MESSAGE-SIZE: 4 bytes
//
// - REQUEST-HEADER:
//   - API-KEY: 2 bytes
//   - API-VERSION: 2 bytes
//   - CORRELATION-ID: 4 bytes
//   - CLIENT-ID
//     - LENGTH: 2 bytes
//     - CONTENT: length bytes
//   - TAGGED-FIELDS: 1 byte, probably 0x00
//
// - REQUEST-BODY: according to the V4 API version
//   - CLIENT-ID
//     - LENGTH: 2 bytes
//     - CONTENT: length bytes
//   - CLIENT-SOFTWARE-VERSION
//     - LENGTH: 1 byte
//     - CONTENT: length bytes
//   - TAGGED-FIELDS: 1 byte, probably 0x00

func readMessage(conn net.Conn) ([]byte, error) {
	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, sizeBuf); err != nil {
		return nil, err
	}

	size := int32(binary.BigEndian.Uint32(sizeBuf))

	message := make([]byte, size)
	if _, err := io.ReadFull(conn, message); err != nil {
		return nil, err
	}

	return message, nil
}

// RESPONSE MESSAGE
// - MESSAGE-SIZE: 4 bytes
//
// - RESPONSE-HEADER:
//   - CORRELATION-ID: 4 bytes
//
// - RESPONSE-BODY:
//   - ERROR-CODE: 2 bytes
//   - API-VERSIONS-ARRAY:
//     - ARRAY-LENGTH: 1 byte
//     - v1:
//       - API-KEY: 2 bytes
//       - MIN-VERSION: 2 bytes
//       - MAX-VERSION: 2 bytes
//       - TAGGED-FIELDS: 1 byte (probably 0x00)
//     - v2:
//   - THROTTLE-TIME: 4 bytes
//   - TAGGED-FIELDS: 1 byte (probably 0x00)

func buildResponse(message []byte) (response []byte, err error) {
	if len(message) < 8 {
		return nil, fmt.Errorf("message too short")
	}

	// apiKey := int16(binary.BigEndian.Uint16(message[0:2]))        // Api Key
	apiVersion := int16(binary.BigEndian.Uint16(message[2:4]))    // Api Version
	correlationID := int32(binary.BigEndian.Uint32(message[4:8])) // Correlation ID

	errorCode := int16(0)
	if apiVersion < 0 || apiVersion > 4 {
		errorCode = 35 // UNSUPPORTED VERSION
	}

	response = binary.BigEndian.AppendUint32(response, uint32(correlationID)) // Correlation ID
	response = binary.BigEndian.AppendUint16(response, uint16(errorCode))     // 16 bytes: Error Code
	response = append(response, byte(2))                                      // 08 bytes: Api Versions Length
	response = binary.BigEndian.AppendUint16(response, uint16(18))            // 16 bytes: Api Key
	response = binary.BigEndian.AppendUint16(response, uint16(3))             // 16 bytes: Min Version
	response = binary.BigEndian.AppendUint16(response, uint16(4))             // 16 bytes: Max Version
	response = append(response, byte(0))                                      // 01 byte: Tagged Fields
	response = binary.BigEndian.AppendUint32(response, uint32(0))             // 32 bytes: Throttle Time
	response = append(response, byte(0))                                      // 01 byte: Tagged Fields

	messageSize := make([]byte, 4)
	binary.BigEndian.PutUint32(messageSize, uint32(len(response)))

	response = append(messageSize, response...) // Prepend the size of the response

	return response, nil
}

/*
 * REQUEST MESSAGE
 * | SIZE (4 bytes) | HEADER | MESSAGE |
 *
 * HEADER V2: https://kafka.apache.org/protocol.html#protocol_messages
 */
