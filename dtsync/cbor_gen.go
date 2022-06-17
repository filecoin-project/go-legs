// Code generated by github.com/whyrusleeping/cbor-gen. DO NOT EDIT.

package dtsync

import (
	"fmt"
	"io"
	"math"
	"sort"

	cid "github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"
)

var _ = xerrors.Errorf
var _ = cid.Undef
var _ = math.E
var _ = sort.Sort

func (t *Message) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if t.OrigPeer == "" {
		var lengthBufMessage = []byte{131}
		if _, err := w.Write(lengthBufMessage); err != nil {
			return err
		}
	} else {
		var lengthBufMessage = []byte{132}

		if _, err := w.Write(lengthBufMessage); err != nil {
			return err
		}
	}

	scratch := make([]byte, 9)

	// t.Cid (cid.Cid) (struct)

	if err := cbg.WriteCidBuf(scratch, w, t.Cid); err != nil {
		return xerrors.Errorf("failed to write cid field t.Cid: %w", err)
	}

	// t.Addrs ([][]uint8) (slice)
	if len(t.Addrs) > cbg.MaxLength {
		return xerrors.Errorf("Slice value in field t.Addrs was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajArray, uint64(len(t.Addrs))); err != nil {
		return err
	}
	for _, v := range t.Addrs {
		if len(v) > cbg.ByteArrayMaxLen {
			return xerrors.Errorf("Byte array in field v was too long")
		}

		if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajByteString, uint64(len(v))); err != nil {
			return err
		}

		if _, err := w.Write(v[:]); err != nil {
			return err
		}
	}

	// t.ExtraData ([]uint8) (slice)
	if len(t.ExtraData) > cbg.ByteArrayMaxLen {
		return xerrors.Errorf("Byte array in field t.ExtraData was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajByteString, uint64(len(t.ExtraData))); err != nil {
		return err
	}

	if _, err := w.Write(t.ExtraData[:]); err != nil {
		return err
	}

	if len(t.OrigPeer) == 0 {
		return nil
	}

	// t.OrigPeer (string) (string)
	if len(t.OrigPeer) > cbg.MaxLength {
		return xerrors.Errorf("Value in field t.OrigPeer was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len(t.OrigPeer))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string(t.OrigPeer)); err != nil {
		return err
	}
	return nil
}

func (t *Message) UnmarshalCBOR(r io.Reader) error {
	*t = Message{}

	br := cbg.GetPeeker(r)
	scratch := make([]byte, 8)

	maj, extra, err := cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra > 4 {
		return fmt.Errorf("cbor input had too many fields")
	}
	if extra < 3 {
		return fmt.Errorf("cbor input had too few fields")
	}
	var skipOrigPeer bool
	if extra == 3 {
		skipOrigPeer = true
	}

	// t.Cid (cid.Cid) (struct)

	{

		c, err := cbg.ReadCid(br)
		if err != nil {
			return xerrors.Errorf("failed to read cid field t.Cid: %w", err)
		}

		t.Cid = c

	}
	// t.Addrs ([][]uint8) (slice)

	maj, extra, err = cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return err
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("t.Addrs: array too large (%d)", extra)
	}

	if maj != cbg.MajArray {
		return fmt.Errorf("expected cbor array")
	}

	if extra > 0 {
		t.Addrs = make([][]uint8, extra)
	}

	for i := 0; i < int(extra); i++ {
		{
			var maj byte
			var extra uint64
			var err error

			maj, extra, err = cbg.CborReadHeaderBuf(br, scratch)
			if err != nil {
				return err
			}

			if extra > cbg.ByteArrayMaxLen {
				return fmt.Errorf("t.Addrs[i]: byte array too large (%d)", extra)
			}
			if maj != cbg.MajByteString {
				return fmt.Errorf("expected byte array")
			}

			if extra > 0 {
				t.Addrs[i] = make([]uint8, extra)
			}

			if _, err := io.ReadFull(br, t.Addrs[i][:]); err != nil {
				return err
			}
		}
	}

	// t.ExtraData ([]uint8) (slice)

	maj, extra, err = cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return err
	}

	if extra > cbg.ByteArrayMaxLen {
		return fmt.Errorf("t.ExtraData: byte array too large (%d)", extra)
	}
	if maj != cbg.MajByteString {
		return fmt.Errorf("expected byte array")
	}

	if extra > 0 {
		t.ExtraData = make([]uint8, extra)
	}

	if _, err := io.ReadFull(br, t.ExtraData[:]); err != nil {
		return err
	}

	// t.OrigPeer (string) (string)
	if !skipOrigPeer {
		sval, err := cbg.ReadStringBuf(br, scratch)
		if err != nil {
			return err
		}

		t.OrigPeer = string(sval)
	}

	return nil
}
