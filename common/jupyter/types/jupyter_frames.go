package types

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/google/uuid"
	"github.com/zhangjyr/distributed-notebook/common/jupyter"
	"github.com/zhangjyr/distributed-notebook/common/utils"
)

const (
	JupyterSignatureScheme = "hmac-sha256"
)

const (
	JupyterFrameStart int = iota
	JupyterFrameSignature
	JupyterFrameHeader
	JupyterFrameParentHeader
	JupyterFrameMetadata
	JupyterFrameContent
	JupyterFrameBuffers
)

var (
	JupyterFrameIDSMSG = []byte("<IDS|MSG>")
	JupyterFrameEmpty  = []byte("{}")
)

// JupyterFrame is a simple wrapper around a byte slice to provide a simple interface for encoding/decoding.
type JupyterFrame []byte

func jupyterFrame(frame []byte) *JupyterFrame {
	jFrame := JupyterFrame(frame)
	return &jFrame
}

func (frame *JupyterFrame) Frame() []byte {
	return *frame
}

func (frame *JupyterFrame) Set(data []byte) {
	*frame = data
}

func (frame *JupyterFrame) Encode(in any) (err error) {
	*frame, err = json.Marshal(in)
	return
}

func (frame *JupyterFrame) Decode(out any) (err error) {
	return json.Unmarshal(*frame, out)
}

// JupyterFrames provides a simple way to access the frames of a Jupyter message.
// A valid JupyterFrames will have at least 5 frames, call Validate() to check before calling other methods.
// 0: <IDS|MSG>, 1: Signature, 2: Header, 3: ParentHeader, 4: Metadata, 5: Content[, 6: Buffers]
type JupyterFrames [][]byte

func NewJupyterFrames(numExtraFrames int) JupyterFrames {
	return NewJupyterFramesWithReservation(0)
}

func NewJupyterFramesWithReservation(numReserved int) JupyterFrames {
	frames := make(JupyterFrames, JupyterFrameContent+1, numReserved+JupyterFrameBuffers+1)
	frames[JupyterFrameStart] = JupyterFrameIDSMSG
	frames[JupyterFrameSignature] = JupyterFrameEmpty
	frames[JupyterFrameHeader] = JupyterFrameEmpty
	frames[JupyterFrameParentHeader] = JupyterFrameEmpty
	frames[JupyterFrameMetadata] = JupyterFrameEmpty
	frames[JupyterFrameContent] = JupyterFrameEmpty
	return frames
}

func NewJupyterFramesWithHeader(msgType string, session string) JupyterFrames {
	frames := NewJupyterFramesWithReservation(1)
	frames.EncodeHeader(&MessageHeader{
		MsgID:    uuid.New().String(),
		Username: MessageHeaderDefaultUsername,
		Session:  session,
		Date:     time.Now().UTC().Format(time.RFC3339Nano),
		MsgType:  msgType,
		Version:  SMRVersion,
	})
	return frames
}

func (frames JupyterFrames) String() string {
	if len(frames) == 0 {
		return "[]"
	}

	s := "["
	for i, frame := range frames {
		s += "\"" + string(frame) + "\""

		if i+1 < len(frames) {
			s += ", "
		}
	}

	s += "]"

	return s
}

func (frames JupyterFrames) Validate() error {
	if len(frames) < 5 /* 6, but buffers are optional, so 5 */ {
		return ErrInvalidJupyterMessage
	}
	return nil
}

func (frames JupyterFrames) Verify(signatureScheme string, key []byte) error {
	if err := frames.Validate(); err != nil {
		fmt.Printf(utils.RedStyle.Render("[ERROR] Failed to validate message frames while verifying message: %v"), err)
		return err
	} else if signatureScheme != JupyterSignatureScheme {
		return ErrNotSupportedSignatureScheme
	} else if !frames.verify(key) {
		return ErrInvalidJupyterSignature
	}
	return nil
}

func (frames JupyterFrames) Sign(signatureScheme string, key []byte) ([][]byte, error) {
	if signatureScheme != JupyterSignatureScheme {
		return frames, ErrNotSupportedSignatureScheme
	}

	signature := frames.sign(key)
	encodeLen := hex.EncodedLen(len(signature))
	if cap(frames[JupyterFrameSignature]) < encodeLen {
		frames[JupyterFrameSignature] = make([]byte, encodeLen)
	}
	hex.Encode(frames[JupyterFrameSignature], signature)
	return frames, nil
}

func (frames JupyterFrames) SignByConnectionInfo(connInfo *ConnectionInfo) ([][]byte, error) {
	return frames.Sign(connInfo.SignatureScheme, []byte(connInfo.Key))
}

func (frames JupyterFrames) HeaderFrame() *JupyterFrame {
	return jupyterFrame(frames[JupyterFrameHeader])
}

func (frames JupyterFrames) GetMessageType() (string, error) {
	var header MessageHeader
	err := json.Unmarshal(frames[JupyterFrameHeader], &header)
	if err != nil {
		return "", err
	}

	return header.MsgType, nil
}

func (frames JupyterFrames) EncodeHeader(in any) (err error) {
	frames[JupyterFrameHeader], err = json.Marshal(in)
	return err
}

func (frames JupyterFrames) DecodeHeader(out any) error {
	return json.Unmarshal(frames[JupyterFrameHeader], out)
}

func (frames JupyterFrames) ParentHeaderFrame() *JupyterFrame {
	return jupyterFrame(frames[JupyterFrameParentHeader])
}

func (frames JupyterFrames) EncodeParentHeader(in any) (err error) {
	frames[JupyterFrameParentHeader], err = json.Marshal(in)
	return err
}

func (frames JupyterFrames) DecodeParentHeader(out any) error {
	return json.Unmarshal(frames[JupyterFrameParentHeader], out)
}

func (frames JupyterFrames) MetadataFrame() *JupyterFrame {
	return jupyterFrame(frames[JupyterFrameMetadata])
}

func (frames JupyterFrames) EncodeMetadata(in any) (err error) {
	frames[JupyterFrameMetadata], err = json.Marshal(in)
	return err
}

func (frames JupyterFrames) DecodeMetadata(out any) error {
	return json.Unmarshal(frames[JupyterFrameMetadata], out)
}

func (frames JupyterFrames) ContentFrame() *JupyterFrame {
	return jupyterFrame(frames[JupyterFrameContent])
}

func (frames JupyterFrames) EncodeContent(in any) (err error) {
	frames[JupyterFrameContent], err = json.Marshal(in)
	return err
}

func (frames JupyterFrames) DecodeContent(out any) error {
	return json.Unmarshal(frames[JupyterFrameContent], out)
}

func (frames JupyterFrames) DecodeBuffers(out any) error {
	if len(frames) > JupyterFrameBuffers {
		return json.Unmarshal(frames[JupyterFrameBuffers], out)
	} else {
		return ErrInvalidJupyterMessage
	}
}

func (frames JupyterFrames) BuffersFrame() *JupyterFrame {
	if len(frames) > JupyterFrameBuffers {
		return jupyterFrame(frames[JupyterFrameBuffers])
	} else {
		return nil
	}
}

func (frames JupyterFrames) verify(signkey []byte) bool {
	expect := frames.sign(signkey)
	signature := make([]byte, hex.DecodedLen(len(frames[JupyterFrameSignature])))
	hex.Decode(signature, frames[JupyterFrameSignature])
	return hmac.Equal(expect, signature)
}

func (frames JupyterFrames) CreateSignature(signatureScheme string, key []byte, offset int) ([]byte, error) {
	if err := frames.Validate(); err != nil {
		fmt.Printf(utils.RedStyle.Render("[ERROR] Failed to validate message frames while creating message signature: %v"), err)
		return nil, err
	} else if signatureScheme != JupyterSignatureScheme {
		return nil, ErrNotSupportedSignatureScheme
	}
	return frames.signWithOffset(key, offset), nil
}

func (frames JupyterFrames) signWithOffset(signkey []byte, offset int) []byte {
	mac := hmac.New(sha256.New, signkey)
	for _, msgpart := range frames[JupyterFrameHeader+offset:] {
		mac.Write(msgpart)
	}
	return mac.Sum(nil)
}

func (frames JupyterFrames) sign(signkey []byte) []byte {
	mac := hmac.New(sha256.New, signkey)
	for _, msgpart := range frames[JupyterFrameHeader:] {
		mac.Write(msgpart)
	}
	return mac.Sum(nil)
}

func SkipIdentitiesFrame(frames [][]byte) (JupyterFrames, int) {
	if len(frames) == 0 {
		return frames, 0
	}

	i := 0
	// Jupyter messages start from "<IDS|MSG>" frame.
	for i < len(frames) && string(frames[i]) != "<IDS|MSG>" {
		i++
	}
	return frames[i:], i
}

// Given a jOffset, attempt to extract a DestFrame from the given set of frames.
func ExtractDestFrameWithOffset(frames [][]byte, jOffset int) (destID string, reqID string) {
	matches := jupyter.ZMQDestFrameRecognizer.FindStringSubmatch(string(frames[jOffset-1]))
	if len(matches) > 0 {
		destID = matches[1]
		reqID = matches[2]
	}

	return
}

// First, determine the offset.
// Next, attempt to extract a DestFrame from the given set of frames.
func ExtractDestFrame(frames [][]byte) (destID string, reqID string, jOffset int) {
	_, jOffset = SkipIdentitiesFrame(frames)
	if jOffset > 0 {
		destID, reqID = ExtractDestFrameWithOffset(frames, jOffset)
	}
	return
}

// GenerateKernelFrame appends a frame contains the kernel ID to the given ZMQ frames.
func AddDestFrame(frames [][]byte, destID string, offsetArg int) (newFrames [][]byte, reqID string, jOffset int) {
	// Automatically detect the dest frame.
	if offsetArg == jupyter.JOffsetAutoDetect {
		_, reqID, jOffset = ExtractDestFrame(frames)
		// If the dest frame is already there, we are done.
		if reqID != "" {
			// s.Log.Debug("Destination frame found. ReqID: %s", reqID)
			return frames, reqID, jOffset
		}
	} else {
		// Don't auto-detect the dest frame.
		jOffset = offsetArg
	}

	// Add dest frame just before "<IDS|MSG>" frame.
	newFrames = append(frames, nil) // Let "append" allocate a new slice if necessary.
	copy(newFrames[jOffset+1:], frames[jOffset:])
	reqID = uuid.New().String()
	newFrames[jOffset] = []byte(fmt.Sprintf(jupyter.ZMQDestFrameFormatter, destID, reqID))

	return newFrames, reqID, jOffset
}

func RemoveDestFrame(frames [][]byte, jOffset int) (removed [][]byte) {
	// Automatically detect the dest frame.
	if jOffset == jupyter.JOffsetAutoDetect {
		var reqID string
		_, reqID, jOffset = ExtractDestFrame(frames)
		// If the dest frame is not available, we are done.
		if reqID == "" {
			return frames
		}
	}

	// Remove dest frame.
	if jOffset > 0 {
		copy(frames[jOffset-1:], frames[jOffset:])
		frames[len(frames)-1] = nil
		frames = frames[:len(frames)-1]
	}
	return frames
}

func ExtractSourceKernelFrame(frames [][]byte) (kernelID string, jOffset int) {
	matches := jupyter.ZMQSourceKernelFrameRecognizer.FindStringSubmatch(string(frames[0]))
	if len(matches) > 0 {
		kernelID = matches[1]
	}

	return
}

func AddSourceKernelFrame(frames [][]byte, kernelID string, jOffset int) (newFrames [][]byte) {
	// Add "source kernel" frame to the very beginning.
	newFrames = append(frames, nil) // Let "append" allocate a new slice if necessary.
	copy(newFrames[1:], frames[0:])
	newFrames[0] = []byte(fmt.Sprintf(jupyter.ZMQSourceKernelFrameFormatter, kernelID))
	return
}

func RemoveSourceKernelFrame(frames [][]byte, jOffset int) (removed [][]byte) {
	existingKernelID, _ := ExtractSourceKernelFrame(frames)
	if existingKernelID != "" {
		return frames[1:]
	}

	return frames
}

func HeaderFromFrames(frames [][]byte) (*MessageHeader, error) {
	jFrames := JupyterFrames(frames)
	if err := jFrames.Validate(); err != nil {
		return nil, err
	}

	var header MessageHeader
	if err := jFrames.DecodeHeader(&header); err != nil {
		return nil, err
	}

	return &header, nil
}

func HeaderFromMsg(msg *zmq4.Msg) (kernelId string, header *MessageHeader, offset int, err error) {
	kernelId, _, offset = ExtractDestFrame(msg.Frames)

	header, err = HeaderFromFrames(msg.Frames[offset:])

	return kernelId, header, offset, err
}

func ValidateFrames(signkey []byte, signatureScheme string, offset int, frames JupyterFrames) bool {
	expect, err := frames.CreateSignature(signatureScheme, signkey, offset)
	if err != nil {
		return false
	}

	signature := make([]byte, hex.DecodedLen(len(frames[offset+JupyterFrameSignature])))
	hex.Decode(signature, frames[offset+JupyterFrameSignature])
	return hmac.Equal(expect, signature)
}
