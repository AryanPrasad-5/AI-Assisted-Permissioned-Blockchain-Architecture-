package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	mp "github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric/protoutil"
)

func TestLeafCertificateValidityWindow(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fixture"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true, IsCA: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	identityBytes, err := proto.Marshal(&mp.SerializedIdentity{Mspid: "FixtureMSP", IdBytes: pemEncode(cert.Raw)})
	if err != nil {
		t.Fatal(err)
	}
	if err := validateLeafValidity(identityBytes, now); err != nil {
		t.Fatalf("valid certificate rejected: %v", err)
	}
	if err := validateLeafValidity(identityBytes, now.Add(2*time.Hour)); err == nil {
		t.Fatal("expired certificate accepted")
	}
	if err := validateLeafValidity(identityBytes, now.Add(-2*time.Hour)); err == nil {
		t.Fatal("not-yet-valid certificate accepted")
	}
}

func pemEncode(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func selfSignedRootPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	template := &x509.Certificate{SerialNumber: big.NewInt(31337),
		Subject: pkix.Name{CommonName: "untrusted-test-root"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		IsCA: true, BasicConstraintsValid: true, MaxPathLen: 1}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pemEncode(der)
}

func fixtureInputs(t *testing.T) ([]byte, []byte) {
	t.Helper()
	block, err := os.ReadFile(os.Getenv("FABRIC_VERIFIER_CONFIG_BLOCK"))
	if err != nil {
		t.Fatalf("config block fixture: %v", err)
	}
	env, err := os.ReadFile(os.Getenv("FABRIC_VERIFIER_SIGNED_ENVELOPE"))
	if err != nil {
		t.Fatalf("signed envelope fixture: %v", err)
	}
	if len(block) == 0 || len(env) == 0 {
		t.Fatal("empty actual channel fixture")
	}
	return block, env
}

func TestActualChannelOrdererAdminSignatureAndPolicy(t *testing.T) {
	block, env := fixtureInputs(t)
	out := verify(block, env, "e1-cfg001", 41)
	if !out.Valid || !out.NativePolicyPass || out.BeforeValue != 40 || out.AfterValue != 41 || out.Signatures != 1 || len(out.SignerMSPIDs) != 1 || out.SignerMSPIDs[0] != "OrdererMSP" {
		t.Fatalf("actual OrdererMSP Admin fixture failed native validation: %+v", out)
	}
}

func TestRejectsOrdererAdminSignatureUnderUntrustedRoot(t *testing.T) {
	blockBytes, env := fixtureInputs(t)
	block := &cb.Block{}
	if err := proto.Unmarshal(blockBytes, block); err != nil {
		t.Fatal(err)
	}
	outer := &cb.Envelope{}
	if err := proto.Unmarshal(block.Data.Data[0], outer); err != nil {
		t.Fatal(err)
	}
	payload := &cb.Payload{}
	if err := proto.Unmarshal(outer.Payload, payload); err != nil {
		t.Fatal(err)
	}
	configEnvelope := &cb.ConfigEnvelope{}
	if err := proto.Unmarshal(payload.Data, configEnvelope); err != nil {
		t.Fatal(err)
	}
	ordererOrg := configEnvelope.Config.ChannelGroup.Groups["Orderer"].Groups["OrdererOrg"]
	configValue := ordererOrg.Values["MSP"]
	mspConfig := &mp.MSPConfig{}
	if err := proto.Unmarshal(configValue.Value, mspConfig); err != nil {
		t.Fatal(err)
	}
	fabricMSP := &mp.FabricMSPConfig{}
	if err := proto.Unmarshal(mspConfig.Config, fabricMSP); err != nil {
		t.Fatal(err)
	}
	fabricMSP.RootCerts = [][]byte{selfSignedRootPEM(t)}
	mspConfig.Config, _ = proto.Marshal(fabricMSP)
	configValue.Value, _ = proto.Marshal(mspConfig)
	configEnvelope.Config.ChannelGroup.Groups["Orderer"].Groups["OrdererOrg"].Values["MSP"] = configValue
	payload.Data, _ = proto.Marshal(configEnvelope)
	outer.Payload, _ = proto.Marshal(payload)
	block.Data.Data[0], _ = proto.Marshal(outer)
	mutatedBlock, err := proto.Marshal(block)
	if err != nil {
		t.Fatal(err)
	}
	out := verify(mutatedBlock, env, "e1-cfg001", 41)
	if out.Valid {
		t.Fatalf("Orderer Admin signature was accepted under unrelated self-signed root: %+v", out)
	}
}

func TestActualFixtureRejectsWrongChannelAndTarget(t *testing.T) {
	block, env := fixtureInputs(t)
	if verify(block, env, "e1-r002", 41).Valid {
		t.Fatal("wrong channel accepted")
	}
	if verify(block, env, "e1-cfg001", 42).Valid {
		t.Fatal("wrong requested target accepted")
	}
}

func TestActualOrg1AdminRejectedByOrdererModificationPolicy(t *testing.T) {
	block, _ := fixtureInputs(t)
	org1, err := os.ReadFile(os.Getenv("FABRIC_VERIFIER_ORG1_ENVELOPE"))
	if err != nil {
		t.Fatalf("Org1 fixture: %v", err)
	}
	out := verify(block, org1, "e1-cfg001", 42)
	if out.Valid || !strings.Contains(out.Error, "policy") {
		t.Fatalf("valid but unauthorized Org1MSP Admin was not rejected by native policy: %+v", out)
	}
}

func TestActualNativeValidatorRejectsStaleReadSet(t *testing.T) {
	block, _ := fixtureInputs(t)
	stale, err := os.ReadFile(os.Getenv("FABRIC_VERIFIER_STALE_ENVELOPE"))
	if err != nil {
		t.Fatalf("stale fixture: %v", err)
	}
	out := verify(block, stale, "e1-cfg001", 44)
	if out.Valid || !strings.Contains(out.Error, "ReadSet") {
		t.Fatalf("stale read set not rejected by native validator: %+v", out)
	}
}

func TestRejectsSignedUnrelatedConfigModification(t *testing.T) {
	block, _ := fixtureInputs(t)
	unrelated, err := os.ReadFile(os.Getenv("FABRIC_VERIFIER_UNRELATED_ENVELOPE"))
	if err != nil {
		t.Fatalf("unrelated fixture: %v", err)
	}
	out := verify(block, unrelated, "e1-cfg001", 43)
	if out.Valid || !strings.Contains(out.Error, "outside Orderer.BatchSize.MaxMessageCount") {
		t.Fatalf("signed unrelated BatchTimeout change not rejected: %+v", out)
	}
}

func TestRejectsMalformedEnvelope(t *testing.T) {
	block, _ := fixtureInputs(t)
	if verify(block, []byte("not protobuf"), "e1-cfg001", 41).Valid {
		t.Fatal("malformed envelope accepted")
	}
	if verify(block, nil, "e1-cfg001", 41).Valid {
		t.Fatal("empty envelope accepted")
	}
}

func TestRejectsAlteredSignedPayload(t *testing.T) {
	block, raw := fixtureInputs(t)
	env := &cb.Envelope{}
	if err := proto.Unmarshal(raw, env); err != nil {
		t.Fatal(err)
	}
	if len(env.Payload) == 0 {
		t.Fatal("fixture missing payload")
	}
	env.Payload[len(env.Payload)-1] ^= 0x01
	mutated, err := proto.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	if verify(block, mutated, "e1-cfg001", 41).Valid {
		t.Fatal("altered signed payload accepted")
	}
}

func TestFabricConfigSignatureBindsExactHeaderAndUpdateBytes(t *testing.T) {
	_, raw := fixtureInputs(t)
	env := &cb.Envelope{}
	if err := proto.Unmarshal(raw, env); err != nil {
		t.Fatal(err)
	}
	payload := &cb.Payload{}
	if err := proto.Unmarshal(env.Payload, payload); err != nil {
		t.Fatal(err)
	}
	update := &cb.ConfigUpdateEnvelope{}
	if err := proto.Unmarshal(payload.Data, update); err != nil {
		t.Fatal(err)
	}
	signed, err := protoutil.ConfigUpdateEnvelopeAsSignedData(update)
	if err != nil || len(signed) != 1 {
		t.Fatalf("native signed-data conversion failed: %v", err)
	}
	if !bytes.Equal(signed[0].Data, append(append([]byte{}, update.Signatures[0].SignatureHeader...), update.ConfigUpdate...)) {
		t.Fatal("Fabric signed bytes are not exact SignatureHeader || ConfigUpdate")
	}
	serialized := &mp.SerializedIdentity{}
	if err := proto.Unmarshal(signed[0].Identity, serialized); err != nil {
		t.Fatal(err)
	}
	certBlock, _ := pem.Decode(serialized.IdBytes)
	if certBlock == nil {
		t.Fatal("signer certificate is not PEM")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := cert.CheckSignature(x509.ECDSAWithSHA256, signed[0].Data, signed[0].Signature); err != nil {
		t.Fatalf("signature does not verify over Fabric's original byte concatenation: %v", err)
	}

	changedUpdate := proto.Clone(update).(*cb.ConfigUpdateEnvelope)
	changedUpdate.ConfigUpdate = append([]byte{}, changedUpdate.ConfigUpdate...)
	changedUpdate.ConfigUpdate[len(changedUpdate.ConfigUpdate)-1] ^= 1
	changedData, err := protoutil.ConfigUpdateEnvelopeAsSignedData(changedUpdate)
	if err != nil {
		t.Fatal(err)
	}
	if cert.CheckSignature(x509.ECDSAWithSHA256, changedData[0].Data, changedData[0].Signature) == nil {
		t.Fatal("signature was reusable after ConfigUpdate alteration")
	}

	changedHeader := proto.Clone(update).(*cb.ConfigUpdateEnvelope)
	header := &cb.SignatureHeader{}
	if err := proto.Unmarshal(changedHeader.Signatures[0].SignatureHeader, header); err != nil {
		t.Fatal(err)
	}
	header.Nonce = append([]byte{}, header.Nonce...)
	header.Nonce[0] ^= 1
	changedHeader.Signatures[0].SignatureHeader, err = proto.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	headerData, err := protoutil.ConfigUpdateEnvelopeAsSignedData(changedHeader)
	if err != nil {
		t.Fatal(err)
	}
	if cert.CheckSignature(x509.ECDSAWithSHA256, headerData[0].Data, headerData[0].Signature) == nil {
		t.Fatal("signature was reusable after SignatureHeader alteration")
	}
}

func TestRejectsDuplicateSignerIdentity(t *testing.T) {
	block, raw := fixtureInputs(t)
	env := &cb.Envelope{}
	if err := proto.Unmarshal(raw, env); err != nil {
		t.Fatal(err)
	}
	payload := &cb.Payload{}
	if err := proto.Unmarshal(env.Payload, payload); err != nil {
		t.Fatal(err)
	}
	update := &cb.ConfigUpdateEnvelope{}
	if err := proto.Unmarshal(payload.Data, update); err != nil {
		t.Fatal(err)
	}
	if len(update.Signatures) == 0 {
		t.Fatal("fixture missing ConfigSignature")
	}
	update.Signatures = append(update.Signatures, proto.Clone(update.Signatures[0]).(*cb.ConfigSignature))
	data, err := proto.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	payload.Data = data
	env.Payload, err = proto.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	mutated, err := proto.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	out := verify(block, mutated, "e1-cfg001", 41)
	if out.Valid || out.Error == "" {
		t.Fatalf("duplicate signer not rejected: %+v", out)
	}
}

func TestRejectsMissingConfigSignature(t *testing.T) {
	block, raw := fixtureInputs(t)
	env := &cb.Envelope{}
	if err := proto.Unmarshal(raw, env); err != nil {
		t.Fatal(err)
	}
	payload := &cb.Payload{}
	if err := proto.Unmarshal(env.Payload, payload); err != nil {
		t.Fatal(err)
	}
	update := &cb.ConfigUpdateEnvelope{}
	if err := proto.Unmarshal(payload.Data, update); err != nil {
		t.Fatal(err)
	}
	update.Signatures = nil
	data, err := proto.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	payload.Data = data
	env.Payload, err = proto.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	mutated, err := proto.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	if verify(block, mutated, "e1-cfg001", 41).Valid {
		t.Fatal("missing ConfigSignature accepted")
	}
}
