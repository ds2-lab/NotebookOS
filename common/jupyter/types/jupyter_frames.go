package types

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/jupyter"
	"time"

	"github.com/google/uuid"
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

	// JupyterFrameRequestTrace is the index of the first "buffer" frame,
	// which contains a RequestTrace to track the overhead at each
	// stage of processing the request.
	JupyterFrameRequestTrace = 6
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
type JupyterFrames struct {
	// Frames are the actual frames.
	Frames [][]byte

	// Offset is an integer denoting the offset from the beginning of the Frames array at which point
	// the Jupyter frames begin. Frames[0, Offset] are ZMQ identities are our dest.req frame.
	Offset int
}

func NewJupyterFramesWithReservation(numReserved int) *JupyterFrames {
	frames := make([][]byte, JupyterFrameContent+1, numReserved+JupyterFrameBuffers+1)
	frames[JupyterFrameStart] = JupyterFrameIDSMSG
	frames[JupyterFrameSignature] = JupyterFrameEmpty
	frames[JupyterFrameHeader] = JupyterFrameEmpty
	frames[JupyterFrameParentHeader] = JupyterFrameEmpty
	frames[JupyterFrameMetadata] = JupyterFrameEmpty
	frames[JupyterFrameContent] = JupyterFrameEmpty
	return &JupyterFrames{
		Frames: frames,
		Offset: 0,
	}
}

func NewJupyterFramesWithHeader(msgType string, session string) *JupyterFrames {
	frames := NewJupyterFramesWithReservation(1)
	_ = frames.EncodeHeader(&MessageHeader{
		MsgID:    uuid.New().String(),
		Username: MessageHeaderDefaultUsername,
		Session:  session,
		Date:     time.Now().UTC().Format(time.RFC3339Nano),
		MsgType:  JupyterMessageType(msgType),
		Version:  SMRVersion,
	})
	return frames
}

func NewJupyterFramesWithHeaderAndSpecificMessageId(msgId string, msgType string, session string) *JupyterFrames {
	frames := NewJupyterFramesWithReservation(1)
	_ = frames.EncodeHeader(&MessageHeader{
		MsgID:    msgId,
		Username: MessageHeaderDefaultUsername,
		Session:  session,
		Date:     time.Now().UTC().Format(time.RFC3339Nano),
		MsgType:  JupyterMessageType(msgType),
		Version:  SMRVersion,
	})
	return frames
}

// NewJupyterFramesFromBytes creates a new JupyterFrames struct and returns a pointer to it.
// This function calculates the offset for the JupyterFrames and populates the new struct accordingly.
func NewJupyterFramesFromBytes(frames [][]byte) *JupyterFrames {
	_, offset := SkipIdentitiesFrame(frames)
	return &JupyterFrames{
		Frames: frames,
		Offset: offset,
	}
}

func (frames *JupyterFrames) Len() int {
	if frames.Frames == nil {
		panic("Frames are nil.")
	}

	return len(frames.Frames)
}

// LenWithoutIdentitiesFrame returns the length of the underlying frames after skipping any ZMQ identities.
func (frames *JupyterFrames) LenWithoutIdentitiesFrame(forceRecompute bool) int {
	if forceRecompute {
		f, _ := frames.SkipIdentitiesFrame() // This forces the offset to be recalculated.
		return len(f)
	} else {
		return len((frames.Frames)[frames.Offset:])
	}
}

func (frames *JupyterFrames) String() string {
	if frames.Len() == 0 {
		return "[]"
	}

	s := "["
	for i, frame := range frames.Frames {
		s += "\"" + string(frame) + "\""

		if i+1 < frames.Len() {
			s += ", "
		}
	}

	s += "]"

	return s
}

func (frames *JupyterFrames) Validate() error {
	if frames.Len() < 5 /* 6, but buffers are optional, so 5 */ {
		return ErrInvalidJupyterMessage
	}
	return nil
}

func (frames *JupyterFrames) Verify(signatureScheme string, key []byte) error {
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

func (frames *JupyterFrames) Sign(signatureScheme string, key []byte) ([][]byte, error) {
	if signatureScheme == "" {
		signatureScheme = JupyterSignatureScheme
	}

	if signatureScheme != JupyterSignatureScheme {
		return frames.Frames, ErrNotSupportedSignatureScheme
	}

	// Ensure the offset is up to date.
	frames.SkipIdentitiesFrame()

	signature := frames.sign(key)
	encodeLen := hex.EncodedLen(len(signature))
	if cap((frames.Frames)[frames.Offset+JupyterFrameSignature]) < encodeLen {
		(frames.Frames)[frames.Offset+JupyterFrameSignature] = make([]byte, encodeLen)
	}
	hex.Encode((frames.Frames)[frames.Offset+JupyterFrameSignature], signature)
	return frames.Frames, nil
}

func (frames *JupyterFrames) SignByConnectionInfo(connInfo *ConnectionInfo) ([][]byte, error) {
	signatureScheme := connInfo.SignatureScheme
	if signatureScheme == "" {
		signatureScheme = JupyterSignatureScheme
	}

	return frames.Sign(connInfo.SignatureScheme, []byte(connInfo.Key))
}

func (frames *JupyterFrames) HeaderFrame() *JupyterFrame {
	return jupyterFrame((frames.Frames)[frames.Offset+JupyterFrameHeader])
}

func (frames *JupyterFrames) GetMessageType() (string, error) {
	var header MessageHeader
	err := json.Unmarshal((frames.Frames)[frames.Offset+JupyterFrameHeader], &header)
	if err != nil {
		return "", err
	}

	return string(header.MsgType), nil
}

func (frames *JupyterFrames) EncodeHeader(in any) (err error) {
	(frames.Frames)[frames.Offset+JupyterFrameHeader], err = json.Marshal(in)
	return err
}

func (frames *JupyterFrames) DecodeHeader(out any) error {
	return json.Unmarshal((frames.Frames)[frames.Offset+JupyterFrameHeader], out)
}

func (frames *JupyterFrames) ParentHeaderFrame() *JupyterFrame {
	return jupyterFrame((frames.Frames)[frames.Offset+JupyterFrameParentHeader])
}

func (frames *JupyterFrames) EncodeParentHeader(in any) (err error) {
	(frames.Frames)[frames.Offset+JupyterFrameParentHeader], err = json.Marshal(in)
	return err
}

func (frames *JupyterFrames) DecodeParentHeader(out any) error {
	return json.Unmarshal((frames.Frames)[frames.Offset+JupyterFrameParentHeader], out)
}

func (frames *JupyterFrames) MetadataFrame() *JupyterFrame {
	return jupyterFrame((frames.Frames)[frames.Offset+JupyterFrameMetadata])
}

func (frames *JupyterFrames) EncodeMetadata(in any) (err error) {
	(frames.Frames)[frames.Offset+JupyterFrameMetadata], err = json.Marshal(in)
	return err
}

func (frames *JupyterFrames) DecodeMetadata(out any) error {
	return json.Unmarshal((frames.Frames)[frames.Offset+JupyterFrameMetadata], out)
}

func (frames *JupyterFrames) ContentFrame() *JupyterFrame {
	return jupyterFrame((frames.Frames)[frames.Offset+JupyterFrameContent])
}

func (frames *JupyterFrames) EncodeContent(in any) (err error) {
	(frames.Frames)[frames.Offset+JupyterFrameContent], err = json.Marshal(in)
	return err
}

func (frames *JupyterFrames) DecodeContent(out any) error {
	return json.Unmarshal((frames.Frames)[frames.Offset+JupyterFrameContent], out)
}

func (frames *JupyterFrames) DecodeBuffers(out any) error {
	if len((frames.Frames)[frames.Offset:]) > JupyterFrameBuffers {
		return json.Unmarshal((frames.Frames)[frames.Offset+JupyterFrameBuffers], out)
	} else {
		return ErrInvalidJupyterMessage
	}
}

func (frames *JupyterFrames) BuffersFrame() *JupyterFrame {
	if len((frames.Frames)[frames.Offset:]) > JupyterFrameBuffers {
		return jupyterFrame((frames.Frames)[frames.Offset+JupyterFrameBuffers])
	} else {
		return nil
	}
}

func (frames *JupyterFrames) verify(signKey []byte) bool {
	expect := frames.sign(signKey)
	signature := make([]byte, hex.DecodedLen(len((frames.Frames)[frames.Offset+JupyterFrameSignature])))
	if _, err := hex.Decode(signature, (frames.Frames)[frames.Offset+JupyterFrameSignature]); err != nil {
		fmt.Printf("[ERROR] Failed to decode: %v", err)
		return false
	}
	return hmac.Equal(expect, signature)
}

func (frames *JupyterFrames) CreateSignature(signatureScheme string, key []byte) ([]byte, error) {
	if err := frames.Validate(); err != nil {
		fmt.Printf(utils.RedStyle.Render("[ERROR] Failed to validate message frames while creating message signature: %v"), err)
		return nil, err
	} else if signatureScheme != JupyterSignatureScheme {
		return nil, ErrNotSupportedSignatureScheme
	}
	return frames.sign(key), nil
}

func (frames *JupyterFrames) sign(signKey []byte) []byte {
	mac := hmac.New(sha256.New, signKey)
	for _, msgPart := range (frames.Frames)[JupyterFrameHeader+frames.Offset : JupyterFrameBuffers+frames.Offset] {
		mac.Write(msgPart)
	}
	return mac.Sum(nil)
}

// SkipIdentitiesFrame returns the frames after the ZMQ identities and dest.req frame.
// This will force the Offset field of the target JupyterFrames to be recomputed/updated.
func (frames *JupyterFrames) SkipIdentitiesFrame() ([][]byte, int) {
	if frames.Len() == 0 {
		return frames.Frames, 0
	}

	i := 0
	// Jupyter messages start from "<IDS|MSG>" frame.
	for i < len(frames.Frames) && string((frames.Frames)[i]) != "<IDS|MSG>" {
		i++
	}

	if frames.Offset != i {
		frames.Offset = i
	}

	return frames.Frames[i:], i
}

func SkipIdentitiesFrame(frames [][]byte) ([][]byte, int) {
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

// ExtractDestFrameWithOffset extracts the destination frame with a given offset.
// Given a jOffset, attempt to extract a DestFrame from the given set of frames.
func ExtractDestFrameWithOffset(frames [][]byte, jOffset int) (destID string, reqID string) {
	matches := jupyter.ZMQDestFrameRecognizer.FindStringSubmatch(string(frames[jOffset-1]))
	if len(matches) > 0 {
		destID = matches[1]
		reqID = matches[2]
	}

	return
}

// ExtractDestFrame extracts the destination frame.
// First, determine the offset.
// Next, attempt to extract a DestFrame from the given set of frames.
func (frames *JupyterFrames) ExtractDestFrame(forceRecomputeOffset bool) (destID string, reqID string, jOffset int) {
	if forceRecomputeOffset {
		_, jOffset = frames.SkipIdentitiesFrame()
	} else {
		jOffset = frames.Offset
	}

	if frames.Offset > 0 {
		destID, reqID = ExtractDestFrameWithOffset(frames.Frames, jOffset)
	}
	return
}

// AddDestFrame appends a frame contains the kernel ID to the given ZMQ frames.
func (frames *JupyterFrames) AddDestFrame(destID string, forceRecomputeOffsetBeforeRemoval bool) (reqID string) {
	// Automatically detect the dest frame.
	if forceRecomputeOffsetBeforeRemoval || frames.Offset == jupyter.JOffsetAutoDetect {
		_, reqID, _ = frames.ExtractDestFrame(true)
		// If the dest frame is already there, we are done.
		if reqID != "" {
			// s.Log.Debug("Destination frame found. ReqID: %s", reqID)
			return reqID
		}
	}

	// Add dest frame just before "<IDS|MSG>" frame.
	frames.Frames = append(frames.Frames, nil) // Let "append" allocate a new slice if necessary.
	copy((frames.Frames)[frames.Offset+1:], (frames.Frames)[frames.Offset:])
	reqID = uuid.New().String()
	(frames.Frames)[frames.Offset] = []byte(fmt.Sprintf(jupyter.ZMQDestFrameFormatter, destID, reqID))

	// This will force the Offset field of the target JupyterFrames to be recomputed/updated.
	frames.SkipIdentitiesFrame()

	// Add 1 to the offset before returning, as we just inserted a new frame at the beginning, so the offset should be shifted by one.
	return reqID
}

func (frames *JupyterFrames) RemoveDestFrame(forceRecomputeOffsetBeforeRemoval bool) (removed [][]byte) {
	if forceRecomputeOffsetBeforeRemoval {
		// This will force the Offset field of the target JupyterFrames to be recomputed/updated.
		frames.SkipIdentitiesFrame()
	}

	// Automatically detect the dest frame.
	if forceRecomputeOffsetBeforeRemoval || frames.Offset == jupyter.JOffsetAutoDetect {
		var reqID string
		_, reqID, _ = frames.ExtractDestFrame(true)
		// If the dest frame is not available, we are done.
		if reqID == "" {
			return frames.Frames
		}

		fmt.Printf("RequestID: \"%s\"\n", reqID)
	}

	fmt.Printf("Removing dest frame at offset %d.\n", frames.Offset)

	// Remove dest frame.
	if frames.Offset > 0 {
		copy((frames.Frames)[frames.Offset-1:], (frames.Frames)[frames.Offset:])
		(frames.Frames)[len(frames.Frames)-1] = nil
		frames.Frames = (frames.Frames)[:len(frames.Frames)-1]
	}

	// This will force the Offset field of the target JupyterFrames to be recomputed/updated.
	frames.SkipIdentitiesFrame()

	return frames.Frames
}

func HeaderFromFrames(frames [][]byte) (*MessageHeader, error) {
	jFrames := NewJupyterFramesFromBytes(frames)
	if err := jFrames.Validate(); err != nil {
		return nil, err
	}

	var header MessageHeader
	if err := jFrames.DecodeHeader(&header); err != nil {
		return nil, err
	}

	return &header, nil
}

func ValidateFrames(signKey []byte, signatureScheme string, frames *JupyterFrames) bool {
	expect, err := frames.CreateSignature(signatureScheme, signKey)
	if err != nil {
		return false
	}

	signature := make([]byte, hex.DecodedLen(len((frames.Frames)[frames.Offset+JupyterFrameSignature])))
	if _, err = hex.Decode(signature, (frames.Frames)[frames.Offset+JupyterFrameSignature]); err != nil {
		return false
	}
	return hmac.Equal(expect, signature)
}
