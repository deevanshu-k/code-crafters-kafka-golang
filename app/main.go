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

	fmt.Printf("> %08b\n", response)

	if _, err := conn.Write(response); err != nil {
		fmt.Println("> Error writing response: ", err.Error())
		return
	}
	fmt.Println("> Response sent successfully")
}

// READS MESSAGES FROM THE CONNECTION
// - First 4 bytes is used to get the size of the message
// - Then read the message of that size
// - Returns the message as a byte slice
func readMessage(conn net.Conn) ([]byte, error) {
	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, sizeBuf); err != nil {
		return nil, err
	}

	size := int32(binary.BigEndian.Uint32((sizeBuf)))

	message := make([]byte, size)
	if _, err := io.ReadFull(conn, message); err != nil {
		return nil, err
	}

	return message, nil
}

func buildResponse(message []byte) (response []byte, err error) {
	if len(message) < 8 {
		return nil, fmt.Errorf("message too short")
	}

	// apiKey := int16(binary.BigEndian.Uint16(message[0:2]))        // Api Key
	apiVersion := int16(binary.BigEndian.Uint16(message[2:4]))    // Api Version
	correlationID := int32(binary.BigEndian.Uint32(message[4:8])) // Correlation ID

	binary.BigEndian.AppendUint32(response, uint32(correlationID)) // Correlation ID

	errorCode := int16(0)
	if 0 < apiVersion || apiVersion > 4 {
		errorCode = 35 // UNSUPPORTED VERSION
	}

	binary.BigEndian.AppendUint16(response, uint16(errorCode)) // 16 bytes: Error Code
	response = append(response, 0x02)                          // 08 bytes: Api Versions Length
	response = append(response, 0x0012)                        // 16 bytes: Api Key
	response = append(response, 0x0003)                        // 16 bytes: Min Version
	response = append(response, 0x0004)                        // 16 bytes: Max Version
	response = append(response, 0x00)                          // 08 bytes: Tagged Fields
	response = append(response, 0x00000000)                    // 32 bytes: Throttle Time
	response = append(response, 0x00)                          // 08 bytes: Tagged Fields

	messageSize := make([]byte, 4)
	binary.BigEndian.PutUint32(messageSize, uint32(len(response)))

	response = append(messageSize, response...) // Prepend the size of the response

	return
}

func encodeUnsignedVarint(value uint32) []byte {
	var buf []byte
	for value >= 0x80 {
		buf = append(buf, byte(value)|0x80)
		value >>= 7
	}
	buf = append(buf, byte(value))
	return buf
}

/*
 * REQUEST MESSAGE
 * | SIZE (4 bytes) | HEADER | MESSAGE |
 *
 * HEADER V2: https://kafka.apache.org/protocol.html#protocol_messages
 */
