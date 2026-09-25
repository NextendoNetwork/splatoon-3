package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"

	"google.golang.org/protobuf/proto"
	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

type saveSearchResult struct {
	UserName   string
	Identifier string
	UID        string
	PID        uint64
}

// searchSaveRecords scans the server's persisted CloudSave protobuf records. The query
// is a case-insensitive substring of either UserName or Identifier.
func searchSaveRecords(dir, query string) ([]saveSearchResult, int, error) {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return nil, 0, fmt.Errorf("search query cannot be empty")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, fmt.Errorf("read save directory %q: %w", dir, err)
	}

	results := make([]saveSearchResult, 0)
	badRecords := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".record.pb") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			badRecords++
			continue
		}
		var record toyohrpb.SaveRecord
		if err := proto.Unmarshal(data, &record); err != nil {
			badRecords++
			continue
		}
		fields := record.GetSaveData().GetFields()
		name := fields["UserName"].GetStringValue()
		identifier := fields["Identifier"].GetStringValue()
		if !strings.Contains(strings.ToLower(name), query) && !strings.Contains(strings.ToLower(identifier), query) {
			continue
		}
		uid := strings.TrimSuffix(entry.Name(), ".record.pb")
		results = append(results, saveSearchResult{UserName: name, Identifier: identifier, UID: uid})
	}
	sort.Slice(results, func(i, j int) bool {
		if strings.EqualFold(results[i].UserName, results[j].UserName) {
			return results[i].UID < results[j].UID
		}
		return strings.ToLower(results[i].UserName) < strings.ToLower(results[j].UserName)
	})
	return results, badRecords, nil
}

func printSaveSearch(query string) int {
	results, badRecords, err := searchSaveRecords(saveDir(), query)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if len(results) == 0 {
		fmt.Printf("No save records matched %q.\n", query)
		if badRecords > 0 {
			fmt.Fprintf(os.Stderr, "Skipped %d unreadable or invalid record(s).\n", badRecords)
		}
		return 0
	}
	pidLookupFailures := 0
	for i := range results {
		pid, err := accountPIDForUID(results[i].UID)
		if err == nil {
			results[i].PID = pid
		} else {
			pidLookupFailures++
		}
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "USERNAME\tIDENTIFIER\tU_SAVE\tPID")
	for _, result := range results {
		pid := "unresolved"
		if result.PID != 0 {
			pid = fmt.Sprint(result.PID)
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", result.UserName, result.Identifier, result.UID, pid)
	}
	_ = w.Flush()
	if badRecords > 0 {
		fmt.Fprintf(os.Stderr, "Skipped %d unreadable or invalid record(s).\n", badRecords)
	}
	if pidLookupFailures > 0 {
		fmt.Fprintf(os.Stderr, "PID unavailable for %d record(s): account missing, or account service lacks /internal/npln-user-id / its shared internal key.\n", pidLookupFailures)
	}
	return 0
}
