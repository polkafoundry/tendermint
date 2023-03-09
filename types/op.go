package types

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"github.com/tendermint/tendermint/crypto/merkle"
	"github.com/tendermint/tendermint/crypto/tmhash"
	tmbytes "github.com/tendermint/tendermint/libs/bytes"
	tmmath "github.com/tendermint/tendermint/libs/math"
	tmproto "github.com/tendermint/tendermint/proto/tendermint/types"
)

// OpKeySize is the size of the transaction key index
const OpKeySize = sha256.Size
const EventUserOperation = "UserOperation"
const EventKeyUserOperation = "UserOperation"

type (
	// Op is an arbitrary byte array.
	// NOTE: Op has no types at this level, so when wire encoded it's just length-prefixed.
	// Might we want types here ?
	Op []byte

	// OpKey is the fixed length array key used as an index.
	OpKey [OpKeySize]byte
)

// Hash computes the TMHASH hash of the wire encoded transaction.
func (op Op) Hash() []byte {
	return tmhash.Sum(op)
}

func (op Op) Key() OpKey {
	return sha256.Sum256(op)
}

// String returns the hex-encoded transaction as a string.
func (op Op) String() string {
	return fmt.Sprintf("Op{%X}", []byte(op))
}

// Ops is a slice of Op.
type Ops []Op

// Hash returns the Merkle root hash of the transaction hashes.
// i.e. the leaves of the tree are the hashes of the ops.
func (ops Ops) Hash() []byte {
	// These allocations will be removed once Ops is switched to [][]byte,
	// ref #2603. This is because golang does not allow type casting slices without unsafe
	opBzs := make([][]byte, len(ops))
	for i := 0; i < len(ops); i++ {
		opBzs[i] = ops[i].Hash()
	}
	return merkle.HashFromByteSlices(opBzs)
}

// Index returns the index of this transaction in the list, or -1 if not found
func (ops Ops) Index(op Op) int {
	for i := range ops {
		if bytes.Equal(ops[i], op) {
			return i
		}
	}
	return -1
}

// IndexByHash returns the index of this transaction hash in the list, or -1 if not found
func (ops Ops) IndexByHash(hash []byte) int {
	for i := range ops {
		if bytes.Equal(ops[i].Hash(), hash) {
			return i
		}
	}
	return -1
}

// Proof returns a simple merkle proof for this node.
// Panics if i < 0 or i >= len(ops)
// TODO: optimize this!
func (ops Ops) Proof(i int) OpProof {
	l := len(ops)
	bzs := make([][]byte, l)
	for i := 0; i < l; i++ {
		bzs[i] = ops[i].Hash()
	}
	root, proofs := merkle.ProofsFromByteSlices(bzs)

	return OpProof{
		RootHash: root,
		Data:     ops[i],
		Proof:    *proofs[i],
	}
}

// OpProof represents a Merkle proof of the presence of a transaction in the Merkle tree.
type OpProof struct {
	RootHash tmbytes.HexBytes `json:"root_hash"`
	Data     Op               `json:"data"`
	Proof    merkle.Proof     `json:"proof"`
}

// Leaf returns the hash(op), which is the leaf in the merkle tree which this proof refers to.
func (tp OpProof) Leaf() []byte {
	return tp.Data.Hash()
}

// Validate verifies the proof. It returns nil if the RootHash matches the dataHash argument,
// and if the proof is internally consistent. Otherwise, it returns a sensible error.
func (tp OpProof) Validate(dataHash []byte) error {
	if !bytes.Equal(dataHash, tp.RootHash) {
		return errors.New("proof matches different data hash")
	}
	if tp.Proof.Index < 0 {
		return errors.New("proof index cannot be negative")
	}
	if tp.Proof.Total <= 0 {
		return errors.New("proof total must be positive")
	}
	valid := tp.Proof.Verify(tp.RootHash, tp.Leaf())
	if valid != nil {
		return errors.New("proof is not internally consistent")
	}
	return nil
}

func (tp OpProof) ToProto() tmproto.OpProof {

	pbProof := tp.Proof.ToProto()

	pbtp := tmproto.OpProof{
		RootHash: tp.RootHash,
		Data:     tp.Data,
		Proof:    pbProof,
	}

	return pbtp
}
func OpProofFromProto(pb tmproto.OpProof) (OpProof, error) {

	pbProof, err := merkle.ProofFromProto(pb.Proof)
	if err != nil {
		return OpProof{}, err
	}

	pbtp := OpProof{
		RootHash: pb.RootHash,
		Data:     pb.Data,
		Proof:    *pbProof,
	}

	return pbtp, nil
}

// ComputeProtoSizeForOps wraps the transactions in tmproto.Data{} and calculates the size.
// https://developers.google.com/protocol-buffers/docs/encoding
func ComputeProtoSizeForOps(ops []Op) int64 {
	data := OpData{Ops: ops}
	pdData := data.ToProto()
	return int64(pdData.Size())
}

// OpData contains the set of transactions included in the block
type OpData struct {
	Ops Ops `json:"ops"`
	// Volatile
	hash tmbytes.HexBytes
}

// Hash returns the hash of the data
func (data *OpData) Hash() tmbytes.HexBytes {
	if data == nil {
		return (Txs{}).Hash()
	}
	if data.hash == nil {
		data.hash = data.Ops.Hash() // NOTE: leaves of merkle tree are TxIDs
	}
	return data.hash
}

// StringIndented returns an indented string representation of the transactions.
func (data *OpData) StringIndented(indent string) string {
	if data == nil {
		return "nil-Data"
	}
	opStrings := make([]string, tmmath.MinInt(len(data.Ops), 21))
	for i, tx := range data.Ops {
		if i == 20 {
			opStrings[i] = fmt.Sprintf("... (%v total)", len(data.Ops))
			break
		}
		opStrings[i] = fmt.Sprintf("%X (%d bytes)", tx.Hash(), len(tx))
	}
	return fmt.Sprintf(`Data{
%s  %v
%s}#%v`,
		indent, strings.Join(opStrings, "\n"+indent+"  "),
		indent, data.hash)
}

// ToProto converts Data to protobuf
func (data *OpData) ToProto() tmproto.OpData {
	tp := new(tmproto.OpData)

	if len(data.Ops) > 0 {
		opBzs := make([][]byte, len(data.Ops))
		for i := range data.Ops {
			opBzs[i] = data.Ops[i]
		}
		tp.Ops = opBzs
	}

	return *tp
}

// OpDataFromProto takes a protobuf representation of Data &
// returns the native type.
func OpDataFromProto(dp *tmproto.OpData) (OpData, error) {
	if dp == nil {
		return OpData{}, errors.New("nil data")
	}
	data := new(OpData)

	if len(dp.Ops) > 0 {
		opBzs := make(Ops, len(dp.Ops))
		for i := range dp.Ops {
			opBzs[i] = dp.Ops[i]
		}
		data.Ops = opBzs
	} else {
		data.Ops = Ops{}
	}

	return *data, nil
}

type PrivBundler interface {
}

type Bundler interface {
	BundleOperation(privBundler PrivBundler, ops Ops) (Txs, error)
}
