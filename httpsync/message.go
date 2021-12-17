package httpsync

import (
	"encoding/json"
	"errors"
	"io"

	ic "github.com/libp2p/go-libp2p-core/crypto"
)

// HeadMsg represents the head cid from the src.
type HeadMsg struct {
	CidStr string
}

// SignedHeadMsgEnvelope is the signed envelope of a HeadMsg. It includes the
// public key of the signer so the receiver can verify it and convert it to a
// peer id. Note, the receiver is not required to use the provided public key.
// Also note the signature is over the whole marshalled headmsg instead of just
// a cidStr. This protects us from forgetting to sign something in the future if
// we add to HeadMsg.
type SignedHeadMsgEnvelope struct {
	HeadMsg []byte
	Sig     []byte
	PubKey  []byte
}

// SignedEnvelope returns the signed and marshalled version of the HeadMsg.
func (m *HeadMsg) SignedEnvelope(privKey ic.PrivKey) ([]byte, error) {
	headMsgBytes, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	sig, err := privKey.Sign(headMsgBytes)
	if err != nil {
		return nil, err
	}

	pubKeyBytes, err := ic.MarshalPublicKey(privKey.GetPublic())
	if err != nil {
		return nil, err
	}

	return json.Marshal(SignedHeadMsgEnvelope{
		HeadMsg: headMsgBytes,
		Sig:     sig,
		PubKey:  pubKeyBytes,
	})
}

// OpenHeadMsgEnvelope returns the HeadMsg from the signed envelope given a
// public key.
func OpenHeadMsgEnvelope(pubKey ic.PubKey, signedHeadMsgEnvelope io.Reader) (HeadMsg, error) {
	var envelop SignedHeadMsgEnvelope
	err := json.NewDecoder(signedHeadMsgEnvelope).Decode(&envelop)
	if err != nil {
		return HeadMsg{}, err
	}

	return openHeadMsgEnvelope(pubKey, envelop)
}

// OpenHeadMsgEnvelopeWithIncludedPubKey verifies the signature with the
// included public key, then returns the public key and HeadMsg. The caller can
// use this public key to derive the signer's peer id.
func OpenHeadMsgEnvelopeWithIncludedPubKey(signedHeadMsgEnvelope io.Reader) (ic.PubKey, HeadMsg, error) {
	var envelop SignedHeadMsgEnvelope
	err := json.NewDecoder(signedHeadMsgEnvelope).Decode(&envelop)
	if err != nil {
		return nil, HeadMsg{}, err
	}

	pubKey, err := ic.UnmarshalPublicKey(envelop.PubKey)
	if err != nil {
		return nil, HeadMsg{}, err
	}

	msg, err := openHeadMsgEnvelope(pubKey, envelop)
	return pubKey, msg, err
}

func openHeadMsgEnvelope(pubKey ic.PubKey, envelop SignedHeadMsgEnvelope) (HeadMsg, error) {
	ok, err := pubKey.Verify(envelop.HeadMsg, envelop.Sig)
	if err != nil {
		return HeadMsg{}, err
	}

	if !ok {
		return HeadMsg{}, errors.New("invalid signature")
	}

	var headMsg HeadMsg
	err = json.Unmarshal(envelop.HeadMsg, &headMsg)
	return headMsg, err
}
