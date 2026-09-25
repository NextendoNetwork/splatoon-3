package main

import (
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"
	commonpb "npln.nintendo.net/npln-practice/proto/common"
	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

func TestSearchSaveRecordsMatchesNameOrIdentifierCaseInsensitively(t *testing.T) {
	dir := t.TempDir()
	writeRecord := func(uid, name, identifier string) {
		t.Helper()
		record := &toyohrpb.SaveRecord{SaveData: &commonpb.MapValue{Fields: map[string]*commonpb.Value{
			"UserName":   {ValueType: &commonpb.Value_StringValue{StringValue: name}},
			"Identifier": {ValueType: &commonpb.Value_StringValue{StringValue: identifier}},
		}}}
		blob, err := proto.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, uid+".record.pb"), blob, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeRecord("u-zara-one", "Zara", "6651")
	writeRecord("u-zara-two", "Another Player", "Zara-6651")
	writeRecord("u-other", "Other", "1111")
	if err := os.WriteFile(filepath.Join(dir, "u-legacy.save"), []byte("not a protobuf record"), 0o600); err != nil {
		t.Fatal(err)
	}

	byName, bad, err := searchSaveRecords(dir, "zArA")
	if err != nil || bad != 0 || len(byName) != 2 {
		t.Fatalf("name search returned %d rows, bad=%d err=%v; want 2", len(byName), bad, err)
	}
	if byName[0].UID != "u-zara-two" || byName[1].UID != "u-zara-one" {
		t.Fatalf("unexpected name search order/results: %+v", byName)
	}
	byIdentifier, _, err := searchSaveRecords(dir, "6651")
	if err != nil || len(byIdentifier) != 2 {
		t.Fatalf("identifier search returned %d rows, err=%v; want 2", len(byIdentifier), err)
	}
}
