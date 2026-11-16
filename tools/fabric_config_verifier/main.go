// fabric_config_verifier validates CONFIG_UPDATE envelopes with the native
// Hyperledger Fabric v2.5.16 channelconfig/configtx/MSP/policy implementation.
// It never contacts a peer or orderer and has no broadcast code path.
package main

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	mp "github.com/hyperledger/fabric-protos-go/msp"
	ob "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/protoutil"
)

type result struct {
	Valid            bool     `json:"valid"`
	ChannelID        string   `json:"channel_id,omitempty"`
	UpdateChannelID  string   `json:"update_channel_id,omitempty"`
	Target           string   `json:"target,omitempty"`
	BeforeValue      uint32   `json:"before_max_message_count,omitempty"`
	AfterValue       uint32   `json:"after_max_message_count,omitempty"`
	ConfigSequence   uint64   `json:"proposed_config_sequence,omitempty"`
	Signatures       int      `json:"signature_count"`
	SignerMSPIDs     []string `json:"signer_msp_ids,omitempty"`
	NativePolicyPass bool     `json:"native_policy_pass,omitempty"`
	ConfigUpdateHash string   `json:"config_update_sha256,omitempty"`
	OuterSigned      bool     `json:"outer_signature_present,omitempty"`
	Error            string   `json:"error,omitempty"`
	FabricSource     string   `json:"fabric_source_commit"`
}

const sourceCommit = "f871cf92a026aba7b12e6f06d71ded3e6e659d71"

func fail(err error) result {
	return result{Valid: false, Error: err.Error(), FabricSource: sourceCommit}
}

func validateLeafValidity(serializedIdentity []byte, now time.Time) error {
	identity := &mp.SerializedIdentity{}
	if err := proto.Unmarshal(serializedIdentity, identity); err != nil {
		return fmt.Errorf("decode serialized identity: %w", err)
	}
	pemBlock, _ := pem.Decode(identity.IdBytes)
	if pemBlock == nil {
		return errors.New("signer identity is not a PEM X.509 certificate")
	}
	cert, err := x509.ParseCertificate(pemBlock.Bytes)
	if err != nil {
		return fmt.Errorf("parse signer X.509 certificate: %w", err)
	}
	if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
		return fmt.Errorf("signer certificate is outside its validity period [%s, %s] at %s",
			cert.NotBefore.UTC().Format(time.RFC3339), cert.NotAfter.UTC().Format(time.RFC3339), now.UTC().Format(time.RFC3339))
	}
	return nil
}

func decodeConfigBlock(raw []byte, expectedChannel string) (string, *cb.Config, error) {
	block := &cb.Block{}
	if err := proto.Unmarshal(raw, block); err != nil {
		return "", nil, fmt.Errorf("decode common.Block: %w", err)
	}
	if block.Data == nil || len(block.Data.Data) == 0 {
		return "", nil, errors.New("config block has no envelope")
	}
	for _, rawEnv := range block.Data.Data {
		env := &cb.Envelope{}
		if err := proto.Unmarshal(rawEnv, env); err != nil {
			continue
		}
		payload, err := protoutil.UnmarshalPayload(env.Payload)
		if err != nil {
			continue
		}
		if payload.Header == nil {
			continue
		}
		ch, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
		if err != nil {
			continue
		}
		if expectedChannel != "" && ch.ChannelId != expectedChannel {
			continue
		}
		if ch.Type != int32(cb.HeaderType_CONFIG) {
			continue
		}
		ce := &cb.ConfigEnvelope{}
		if err := proto.Unmarshal(payload.Data, ce); err != nil {
			return "", nil, fmt.Errorf("decode ConfigEnvelope: %w", err)
		}
		if ce.Config == nil || ce.Config.ChannelGroup == nil {
			return "", nil, errors.New("config block contains incomplete config")
		}
		return ch.ChannelId, ce.Config, nil
	}
	return "", nil, errors.New("no CONFIG envelope for requested channel in block")
}

func getBatch(config *cb.Config) (*ob.BatchSize, error) {
	if config == nil || config.ChannelGroup == nil {
		return nil, errors.New("missing channel config")
	}
	orderer := config.ChannelGroup.Groups[channelconfig.OrdererGroupKey]
	if orderer == nil {
		return nil, errors.New("missing Orderer group")
	}
	value := orderer.Values[channelconfig.BatchSizeKey]
	if value == nil {
		return nil, errors.New("missing Orderer.BatchSize")
	}
	batch := &ob.BatchSize{}
	if err := proto.Unmarshal(value.Value, batch); err != nil {
		return nil, fmt.Errorf("decode BatchSize: %w", err)
	}
	return batch, nil
}

func verify(blockBytes, envelopeBytes []byte, channel string, target uint32) result {
	if channel == "" {
		return fail(errors.New("channel is required"))
	}
	if target == 0 {
		return fail(errors.New("target MaxMessageCount must be positive"))
	}
	blockChannel, current, err := decodeConfigBlock(blockBytes, channel)
	if err != nil {
		return fail(err)
	}
	if blockChannel != channel {
		return fail(fmt.Errorf("channel mismatch: block=%s requested=%s", blockChannel, channel))
	}
	before, err := getBatch(current)
	if err != nil {
		return fail(err)
	}

	env := &cb.Envelope{}
	if err := proto.Unmarshal(envelopeBytes, env); err != nil {
		return fail(fmt.Errorf("decode common.Envelope: %w", err))
	}
	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		return fail(fmt.Errorf("decode envelope payload: %w", err))
	}
	if payload.Header == nil {
		return fail(errors.New("envelope payload lacks header"))
	}
	channelHeader, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return fail(fmt.Errorf("decode channel header: %w", err))
	}
	if channelHeader.Type != int32(cb.HeaderType_CONFIG_UPDATE) {
		return fail(fmt.Errorf("envelope type=%s, want CONFIG_UPDATE", cb.HeaderType_name[channelHeader.Type]))
	}
	if channelHeader.ChannelId != channel {
		return fail(fmt.Errorf("envelope channel=%s, want %s", channelHeader.ChannelId, channel))
	}
	updateEnv := &cb.ConfigUpdateEnvelope{}
	if err := proto.Unmarshal(payload.Data, updateEnv); err != nil {
		return fail(fmt.Errorf("decode ConfigUpdateEnvelope: %w", err))
	}
	if len(updateEnv.ConfigUpdate) == 0 || len(updateEnv.Signatures) == 0 {
		return fail(errors.New("ConfigUpdateEnvelope requires update bytes and at least one signature"))
	}

	seen := map[string]bool{}
	var signerMSPs []string
	for i, sig := range updateEnv.Signatures {
		if sig == nil || len(sig.SignatureHeader) == 0 || len(sig.Signature) == 0 {
			return fail(fmt.Errorf("signature %d is incomplete", i))
		}
		header := &cb.SignatureHeader{}
		if err := proto.Unmarshal(sig.SignatureHeader, header); err != nil {
			return fail(fmt.Errorf("signature %d header: %w", i, err))
		}
		if len(header.Creator) == 0 || len(header.Nonce) == 0 {
			return fail(fmt.Errorf("signature %d has empty creator or nonce", i))
		}
		if err := validateLeafValidity(header.Creator, time.Now()); err != nil {
			return fail(fmt.Errorf("signature %d: %w", i, err))
		}
		key := string(header.Creator)
		if seen[key] {
			return fail(errors.New("duplicate or conflicting signatures from the same serialized identity"))
		}
		seen[key] = true
		identity := &mp.SerializedIdentity{}
		if err := proto.Unmarshal(header.Creator, identity); err != nil {
			return fail(fmt.Errorf("signature %d creator identity: %w", i, err))
		}
		signerMSPs = append(signerMSPs, identity.Mspid)
	}

	// NewBundle creates the MSP manager and policy manager from this exact
	// channel config. ProposeConfigUpdate uses Fabric v2.5.16's native
	// protoutil.ConfigUpdateEnvelopeAsSignedData (SignatureHeader || ConfigUpdate),
	// MSP identity deserialization/validation, signature verification, and policy
	// evaluator while also enforcing read-set versions and mod_policy.
	bundle, err := channelconfig.NewBundle(channel, current, factory.GetDefault())
	if err != nil {
		return fail(fmt.Errorf("construct native channel bundle: %w", err))
	}
	// Verify the outer common.Envelope signature as well. Fabric's configtx
	// authorization evaluates the inner ConfigSignature set; the outer sender
	// signature is separately authenticated by the configured channel MSP.
	if len(env.Signature) == 0 {
		return fail(errors.New("outer common.Envelope signature is missing"))
	}
	outerHeader := &cb.SignatureHeader{}
	if payload.Header.SignatureHeader == nil {
		return fail(errors.New("outer signature header is missing"))
	}
	if err := proto.Unmarshal(payload.Header.SignatureHeader, outerHeader); err != nil {
		return fail(fmt.Errorf("decode outer signature header: %w", err))
	}
	if len(outerHeader.Creator) == 0 || len(outerHeader.Nonce) == 0 {
		return fail(errors.New("outer signature header requires creator and nonce"))
	}
	if err := validateLeafValidity(outerHeader.Creator, time.Now()); err != nil {
		return fail(fmt.Errorf("outer signer: %w", err))
	}
	outerSigned, err := protoutil.EnvelopeAsSignedData(env)
	if err != nil || len(outerSigned) != 1 {
		return fail(fmt.Errorf("decode outer envelope signature: %w", err))
	}
	outerIdentity, err := bundle.MSPManager().DeserializeIdentity(outerSigned[0].Identity)
	if err != nil {
		return fail(fmt.Errorf("deserialize outer signer using channel MSPs: %w", err))
	}
	if err := outerIdentity.Validate(); err != nil {
		return fail(fmt.Errorf("outer signer identity validation failed: %w", err))
	}
	if err := outerIdentity.Verify(outerSigned[0].Data, outerSigned[0].Signature); err != nil {
		return fail(fmt.Errorf("outer envelope cryptographic signature invalid: %w", err))
	}
	proposed, err := bundle.ConfigtxValidator().ProposeConfigUpdate(env)
	if err != nil {
		return fail(fmt.Errorf("Fabric native CONFIG_UPDATE authorization failed: %w", err))
	}
	if proposed == nil || proposed.Config == nil {
		return fail(errors.New("native validator returned no proposed configuration"))
	}
	if channelHeader.ChannelId != channel {
		return fail(errors.New("channel binding changed during validation"))
	}
	if len(updateEnv.ConfigUpdate) == 0 {
		return fail(errors.New("missing serialized ConfigUpdate bytes"))
	}

	// Narrow controller authorization: exactly one config-sequence increment,
	// one BatchSize version increment, and only MaxMessageCount changes.
	expected := proto.Clone(current).(*cb.Config)
	expected.Sequence++
	orderer := expected.ChannelGroup.Groups[channelconfig.OrdererGroupKey]
	batchValue := orderer.Values[channelconfig.BatchSizeKey]
	expectedBatch := &ob.BatchSize{}
	if err := proto.Unmarshal(batchValue.Value, expectedBatch); err != nil {
		return fail(fmt.Errorf("decode expected BatchSize: %w", err))
	}
	if expectedBatch.MaxMessageCount == target {
		return fail(errors.New("requested MaxMessageCount already active; empty transition is not allowed"))
	}
	expectedBatch.MaxMessageCount = target
	encodedBatch, err := proto.Marshal(expectedBatch)
	if err != nil {
		return fail(fmt.Errorf("encode expected BatchSize: %w", err))
	}
	batchValue.Value = encodedBatch
	batchValue.Version++
	if proposed.Config.Sequence != expected.Sequence || !proto.Equal(proposed.Config.ChannelGroup, expected.ChannelGroup) {
		return fail(errors.New("proposed write set changes fields outside Orderer.BatchSize.MaxMessageCount and its required versions"))
	}
	// Also insist the read set was bound to the current Config sequence. The
	// native validator enforces versions of all entries present in that set.
	update := &cb.ConfigUpdate{}
	if err := proto.Unmarshal(updateEnv.ConfigUpdate, update); err != nil {
		return fail(fmt.Errorf("decode signed ConfigUpdate: %w", err))
	}
	if update.ChannelId != channel {
		return fail(fmt.Errorf("ConfigUpdate channel=%s, want %s", update.ChannelId, channel))
	}
	after, err := getBatch(proposed.Config)
	if err != nil {
		return fail(err)
	}
	return result{Valid: true, ChannelID: channel, UpdateChannelID: update.ChannelId,
		Target: "Orderer.BatchSize.MaxMessageCount", BeforeValue: before.MaxMessageCount,
		AfterValue: after.MaxMessageCount, ConfigSequence: proposed.Config.Sequence,
		Signatures: len(updateEnv.Signatures), SignerMSPIDs: signerMSPs,
		NativePolicyPass: true, ConfigUpdateHash: fmt.Sprintf("%x", sha256.Sum256(updateEnv.ConfigUpdate)),
		OuterSigned: true, FabricSource: sourceCommit}
}

// inspectEnvelope is a read-only structural preflight used to preserve and
// compare the opaque serialized ConfigUpdate bytes around peer signconfigtx.
// It deliberately makes no signature, MSP, or policy-validity claim.
func inspectEnvelope(blockBytes, envelopeBytes []byte, channel string) result {
	blockChannel, _, err := decodeConfigBlock(blockBytes, channel)
	if err != nil {
		return fail(err)
	}
	if blockChannel != channel {
		return fail(fmt.Errorf("config block channel=%s, want %s", blockChannel, channel))
	}
	env := &cb.Envelope{}
	if err := proto.Unmarshal(envelopeBytes, env); err != nil {
		return fail(fmt.Errorf("decode common.Envelope: %w", err))
	}
	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		return fail(fmt.Errorf("decode envelope payload: %w", err))
	}
	if payload.Header == nil {
		return fail(errors.New("envelope payload lacks header"))
	}
	ch, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return fail(fmt.Errorf("decode channel header: %w", err))
	}
	if ch.Type != int32(cb.HeaderType_CONFIG_UPDATE) || ch.ChannelId != channel {
		return fail(fmt.Errorf("envelope type/channel mismatch: type=%d channel=%s", ch.Type, ch.ChannelId))
	}
	updateEnv := &cb.ConfigUpdateEnvelope{}
	if err := proto.Unmarshal(payload.Data, updateEnv); err != nil {
		return fail(fmt.Errorf("decode ConfigUpdateEnvelope: %w", err))
	}
	update := &cb.ConfigUpdate{}
	if err := proto.Unmarshal(updateEnv.ConfigUpdate, update); err != nil {
		return fail(fmt.Errorf("decode ConfigUpdate: %w", err))
	}
	if update.ChannelId != channel {
		return fail(fmt.Errorf("ConfigUpdate channel=%s, want %s", update.ChannelId, channel))
	}
	return result{Valid: true, ChannelID: channel, UpdateChannelID: update.ChannelId,
		Signatures: len(updateEnv.Signatures), ConfigUpdateHash: fmt.Sprintf("%x", sha256.Sum256(updateEnv.ConfigUpdate)),
		OuterSigned: len(env.Signature) > 0, FabricSource: sourceCommit}
}

func main() {
	blockPath := flag.String("config-block", "", "read-only current common.Block protobuf")
	envelopePath := flag.String("envelope", "", "unsigned outer CONFIG_UPDATE common.Envelope protobuf containing signed ConfigUpdateEnvelope")
	channel := flag.String("channel", "", "expected channel ID")
	target := flag.Uint("target-max-message-count", 0, "the sole permitted BatchSize target")
	inspectOnly := flag.Bool("inspect-only", false, "structurally inspect channel binding and hash opaque ConfigUpdate bytes; does not verify signatures or policy")
	flag.Parse()
	if *blockPath == "" || *envelopePath == "" || *channel == "" {
		write(fail(errors.New("--config-block, --envelope, and --channel are required")))
		return
	}
	blockBytes, err := os.ReadFile(*blockPath)
	if err != nil {
		write(fail(fmt.Errorf("read config block: %w", err)))
		return
	}
	envelopeBytes, err := os.ReadFile(*envelopePath)
	if err != nil {
		write(fail(fmt.Errorf("read envelope: %w", err)))
		return
	}
	if *inspectOnly {
		write(inspectEnvelope(blockBytes, envelopeBytes, *channel))
		return
	}
	write(verify(blockBytes, envelopeBytes, *channel, uint32(*target)))
}

func write(out result) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
	if !out.Valid {
		os.Exit(2)
	}
}
