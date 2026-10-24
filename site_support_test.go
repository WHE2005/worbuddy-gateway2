package main

import (
	"strings"
	"testing"
	"time"
)

func withTestCatalog(t *testing.T, catalogs map[string][]catalogModel) {
	t.Helper()
	modelsMu.Lock()
	previous := catalogModels
	catalogModels = catalogs
	modelsMu.Unlock()
	t.Cleanup(func() {
		modelsMu.Lock()
		catalogModels = previous
		modelsMu.Unlock()
	})
}

func withTestAccounts(t *testing.T, accs []*Account) {
	t.Helper()
	accountMu.Lock()
	previous := accounts
	previousRR := rrIndex
	accounts = accs
	rrIndex = 0
	accountMu.Unlock()
	t.Cleanup(func() {
		accountMu.Lock()
		accounts = previous
		rrIndex = previousRR
		accountMu.Unlock()
	})
}

func TestModelSiteSupportCatalog(t *testing.T) {
	withTestCatalog(t, map[string][]catalogModel{
		"cn":   {{ID: "cn-only"}, {ID: "both"}},
		"intl": {{ID: "intl-only"}, {ID: "both"}},
	})

	cases := []struct {
		site, model string
		want        siteModelSupport
	}{
		{"cn", "cn-only", siteSupportSupported},
		{"intl", "cn-only", siteSupportUnsupported},
		{"intl", "intl-only", siteSupportSupported},
		{"cn", "intl-only", siteSupportUnsupported},
		{"cn", "both", siteSupportSupported},
		{"intl", "both", siteSupportSupported},
		{"cn", "unknown", siteSupportUnknown},
		{"intl", "unknown", siteSupportUnknown},
	}
	for _, tc := range cases {
		if got := modelSiteSupport(tc.site, tc.model); got != tc.want {
			t.Errorf("modelSiteSupport(%q, %q) = %v, want %v", tc.site, tc.model, got, tc.want)
		}
	}
}

func TestModelSiteSupportDoesNotTreatMissingCatalogAsUnsupported(t *testing.T) {
	withTestCatalog(t, map[string][]catalogModel{
		"cn": {{ID: "cn-only"}},
	})
	if got := modelSiteSupport("intl", "cn-only"); got != siteSupportUnknown {
		t.Fatalf("missing intl catalog must stay unknown, got %v", got)
	}
}

func TestModelSiteSupportUsesServedEvidence(t *testing.T) {
	withTestCatalog(t, map[string][]catalogModel{
		"cn":   {{ID: "cn-only"}},
		"intl": {{ID: "intl-other"}},
	})
	modelsMu.Lock()
	previousServed := servedSiteModels
	servedSiteModels = map[string]bool{}
	modelsMu.Unlock()
	t.Cleanup(func() {
		modelsMu.Lock()
		servedSiteModels = previousServed
		modelsMu.Unlock()
	})

	if got := modelSiteSupport("intl", "cn-only"); got != siteSupportUnsupported {
		t.Fatalf("before evidence intl support=%v, want unsupported", got)
	}
	markSiteModelServed("intl", "cn-only")
	if got := modelSiteSupport("intl", "cn-only"); got != siteSupportSupported {
		t.Fatalf("after served evidence intl support=%v, want supported", got)
	}
	if got := siteSupportBadge(modelSiteSupport("cn", "cn-only"), modelSiteSupport("intl", "cn-only")); got != "both" {
		t.Fatalf("badge=%q, want both", got)
	}
}

func TestModelSiteSupportUsesProbeVerdict(t *testing.T) {
	withTestCatalog(t, map[string][]catalogModel{
		"cn":   {{ID: "cn-only"}},
		"intl": {{ID: "intl-other"}},
	})
	modelsMu.Lock()
	previousServed := servedSiteModels
	previousProbes := modelProbes
	servedSiteModels = map[string]bool{}
	modelProbes = map[string]modelPriceProbe{
		probeKey("intl", "cn-only"): {Verdict: "free"},
	}
	modelsMu.Unlock()
	t.Cleanup(func() {
		modelsMu.Lock()
		servedSiteModels = previousServed
		modelProbes = previousProbes
		modelsMu.Unlock()
	})

	if got := modelSiteSupport("intl", "cn-only"); got != siteSupportSupported {
		t.Fatalf("probe free should count as intl support, got %v", got)
	}
}

func TestSiteSupportBadge(t *testing.T) {
	cases := []struct {
		cn, intl siteModelSupport
		want     string
	}{
		{siteSupportSupported, siteSupportSupported, "both"},
		{siteSupportSupported, siteSupportUnsupported, "cn"},
		{siteSupportUnsupported, siteSupportSupported, "intl"},
		{siteSupportUnknown, siteSupportUnknown, ""},
	}
	for _, tc := range cases {
		if got := siteSupportBadge(tc.cn, tc.intl); got != tc.want {
			t.Errorf("siteSupportBadge(%v, %v) = %q, want %q", tc.cn, tc.intl, got, tc.want)
		}
	}
}

func TestNextAccountSkipsUnsupportedSite(t *testing.T) {
	withTestCatalog(t, map[string][]catalogModel{
		"cn":   {{ID: "cn-only"}},
		"intl": {{ID: "intl-only"}},
	})
	cn := &Account{Path: "cn.json", Auth: &StoredAuth{Edition: "cn"}}
	intl := &Account{Path: "intl.json", Auth: &StoredAuth{Edition: "intl"}}
	withTestAccounts(t, []*Account{intl, cn})
	t.Cleanup(func() { _ = setModelAccountFilter(nil) })
	_ = setModelAccountFilter(nil)

	selected, _, err := nextAccountForModel("cn-only", nil)
	if err != nil || selected != cn {
		t.Fatalf("expected cn account, got %v err=%v", selected, err)
	}

	withTestAccounts(t, []*Account{intl})
	_, _, err = nextAccountForModel("cn-only", nil)
	if err == nil || !strings.Contains(err.Error(), "站点不支持=1") {
		t.Fatalf("expected site-unsupported rejection, got %v", err)
	}
}

func TestModelRowsExposeSiteSupportAndServableCount(t *testing.T) {
	resetModelStats()
	t.Cleanup(resetModelStats)
	withTestCatalog(t, map[string][]catalogModel{
		"cn":   {{ID: "glm-5.3-flashx", HasMultiplier: true, Multiplier: 0.14}},
		"intl": {{ID: "glm-5.3", HasMultiplier: true, Multiplier: 0.79}},
	})
	cn := &Account{Path: "cn.json", Auth: &StoredAuth{Edition: "cn"}}
	intl := &Account{Path: "intl.json", Auth: &StoredAuth{Edition: "intl"}}
	setModelFilter(nil, nil)
	t.Cleanup(func() { setModelFilter(nil, nil) })

	rows := buildAllModelStatSnapshots(time.Now(), []*Account{cn, intl})
	byID := map[string]modelStatSnapshot{}
	for _, row := range rows {
		byID[row.ID] = row
	}

	flashx, ok := byID["glm-5.3-flashx"]
	if !ok {
		t.Fatal("flashx row missing")
	}
	if flashx.SiteSupport != "cn" {
		t.Fatalf("flashx siteSupport=%q, want cn", flashx.SiteSupport)
	}
	if flashx.AvailableAccounts != 1 {
		t.Fatalf("flashx availableAccounts=%d, want 1", flashx.AvailableAccounts)
	}

	glm, ok := byID["glm-5.3"]
	if !ok {
		t.Fatal("glm-5.3 row missing")
	}
	if glm.SiteSupport != "intl" {
		t.Fatalf("glm-5.3 siteSupport=%q, want intl", glm.SiteSupport)
	}
	if glm.AvailableAccounts != 1 {
		t.Fatalf("glm-5.3 availableAccounts=%d, want 1", glm.AvailableAccounts)
	}
}
