package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/imroc/req/v3"
)

const URL_RENDIMENTO_TITULOS = "https://www.tesourodireto.com.br/produtos/dados-sobre-titulos/rendimento-dos-titulos"

func fetchLastMarketPricingDate(client *req.Client) (string, error) {
	resp, err := client.R().
		SetHeader("Accept-Language", "pt-BR,pt;q=0.9").
		Get(URL_RENDIMENTO_TITULOS)
	if err != nil {
		return "", err
	}
	if !resp.IsSuccessState() {
		return "", fmt.Errorf("HTTP %d ao buscar página de rendimento dos títulos", resp.GetStatusCode())
	}

	html := resp.String()

	// A página renderiza o <p class="lastMarketPricingDate"></p> vazio e injeta o valor via JS:
	//   var lastMarketPricingDate = `2026-01-28T13:02:01.613`
	raw, ok := extractJSVar(html, "lastMarketPricingDate")
	if !ok {
		return "", nil // melhor não quebrar o pipeline
	}

	ts, err := parseTDISO(raw)
	if err != nil {
		return "", err
	}

	return ts.Format(time.RFC3339Nano), nil
}

// Extrai: var <name> = `...` (ou "..." / '...')
func extractJSVar(html, name string) (string, bool) {
	re := regexp.MustCompile(`(?m)\bvar\s+` + regexp.QuoteMeta(name) + `\s*=\s*(?:` +
		"`" + `([^` + "`" + `]+)` + "`" +
		`|"([^"]+)"|'([^']+)')`)
	m := re.FindStringSubmatch(html)
	if len(m) == 0 {
		return "", false
	}
	for i := 1; i <= 3; i++ {
		if m[i] != "" {
			return strings.TrimSpace(m[i]), true
		}
	}
	return "", false
}

// Ex.: "2026-01-28T13:02:01.613" (sem timezone no HTML do TD)
func parseTDISO(s string) (time.Time, error) {
	s = strings.TrimSpace(s)

	// Se vier com timezone (Z ou +/-), tenta RFC3339 direto.
	if strings.ContainsAny(s, "Z+-") && strings.Contains(s, "T") && strings.Count(s, ":") >= 2 {
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t, nil
		}
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			return t, nil
		}
	}

	loc, _ := time.LoadLocation("America/Sao_Paulo")

	// com milissegundos
	if t, err := time.ParseInLocation("2006-01-02T15:04:05.000", s, loc); err == nil {
		return t, nil
	}
	// sem fração
	return time.ParseInLocation("2006-01-02T15:04:05", s, loc)
}

const URL_RESGATAR = "https://www.tesourodireto.com.br/documents/d/guest/rendimento-resgatar-csv?download=true"

// ===== Output schema =====

type Meta struct {
	Source            string `json:"source"`
	SourceURL         string `json:"source_url"`
	LastRunAt         string `json:"last_run_at"`
	LastPriceChangeAt string `json:"last_price_change_at"`
	Rows              int    `json:"rows"`
}

type DataRow struct {
	Ticker     string  `json:"Ticker"`
	PrecoAtual float64 `json:"Preco_Atual"`
	YieldAtual float64 `json:"Yield_Atual"`
}

type Payload struct {
	Meta Meta      `json:"meta"`
	Data []DataRow `json:"data"`
}

// ===== Raw CSV row =====

type ResgateRow struct {
	Titulo           string
	RendimentoAnual  string
	PrecoResgate     float64
	VencimentoTitulo string
	RawPrecoResgate  string
}

func main() {
	var contains string
	flag.StringVar(&contains, "contains", "", "filtra linhas cujo título contém esse texto (case-insensitive)")
	flag.Parse()

	client := req.C().
		// Impersona um browser comum (Chrome recente)
		ImpersonateChrome().
		// Timeouts básicos
		SetTimeout(15 * time.Second).
		// Headers típicos de browser
		SetCommonHeaders(map[string]string{
			"Accept":          "text/html,application/json;q=0.9,*/*;q=0.8",
			"Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
			"Cache-Control":   "no-cache",
		})

	// Warm-up: visita a página HTML pra ganhar cookies/sessão antes do CSV
	_, _ = client.R().
		SetHeader("Accept", "text/html,*/*;q=0.8").
		Get(URL_RENDIMENTO_TITULOS)

	// Baixa CSV como texto
	resp, err := client.R().
		SetHeader("Referer", URL_RENDIMENTO_TITULOS).
		Get(URL_RESGATAR)
	if err != nil {
		log.Fatalf("erro ao baixar CSV: %v", err)
	}
	if !resp.IsSuccessState() {
		log.Fatalf("HTTP %d ao baixar CSV", resp.GetStatusCode())
	}
	body := resp.String()

	rows, err := parseResgateCSV(body)
	if err != nil {
		log.Fatalf("erro ao parsear CSV: %v", err)
	}

	if contains != "" {
		rows = filterContains(rows, contains)
	}

	runTS := nowSPISO()

	lastPriceChangeAt, _ := fetchLastMarketPricingDate(client)

	data := make([]DataRow, 0, len(rows))
	for _, r := range rows {
		vencYMD := parsePtBrDateToYMD(r.VencimentoTitulo)
		if vencYMD == "" {
			continue
		}

		base := inferTickerBaseFromTituloTD(r.Titulo)
		ticker := fmt.Sprintf("%s %s", base, vencYMD)

		yld := parseYieldPercentToDecimal(r.RendimentoAnual)
		// Se não conseguir parsear yield, ainda assim manda o preço (yield=0)
		data = append(data, DataRow{
			Ticker:     ticker,
			PrecoAtual: r.PrecoResgate,
			YieldAtual: yld,
		})
	}

	payload := Payload{
		Meta: Meta{
			Source:            "TD_Scrape",
			SourceURL:         URL_RESGATAR,
			LastRunAt:         runTS,             // sempre atualiza
			LastPriceChangeAt: lastPriceChangeAt, // TODO: preencher depois com "última mudança de preço"
			Rows:              len(data),
		},
		Data: data,
	}

	b, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		log.Fatalf("erro marshal json: %v", err)
	}

	outDir := "output"
	if err := os.MkdirAll(outDir, 0755); err != nil {
		log.Fatalf("erro criando pasta output: %v", err)
	}

	outPath := filepath.Join(outDir, "td_realtime_resgatar.json")
	if err := os.WriteFile(outPath, b, 0644); err != nil {
		log.Fatalf("erro salvando json: %v", err)
	}

	fmt.Printf("Salvou em %s\n", outPath)
}

// ===== Helpers: time =====

func nowSPISO() string {
	loc, err := time.LoadLocation("America/Sao_Paulo")
	t := time.Now()
	if err == nil {
		t = t.In(loc)
	}
	return t.Truncate(time.Second).Format(time.RFC3339)
}

// ===== Helpers: filtering =====

func filterContains(rows []ResgateRow, substr string) []ResgateRow {
	substr = strings.ToLower(strings.TrimSpace(substr))
	if substr == "" {
		return rows
	}
	var out []ResgateRow
	for _, r := range rows {
		if strings.Contains(strings.ToLower(r.Titulo), substr) {
			out = append(out, r)
		}
	}
	return out
}

// ===== CSV parsing =====

func parseResgateCSV(csvText string) ([]ResgateRow, error) {
	csvText = strings.TrimSpace(csvText)

	rd := csv.NewReader(strings.NewReader(csvText))
	rd.Comma = ';'
	rd.FieldsPerRecord = -1 // tolerante a variações

	all, err := rd.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(all) < 2 {
		return nil, errors.New("CSV vazio ou sem linhas de dados")
	}

	headers := all[0]
	if len(headers) == 0 {
		return nil, errors.New("CSV sem cabeçalho")
	}
	headers[0] = strings.TrimPrefix(headers[0], "\uFEFF") // remove BOM

	idxTitulo := findHeader(headers, "Título")
	idxRend := findHeader(headers, "Rendimento anual do título")
	idxPreco := findHeader(headers, "Preço unitário de resgate")
	idxVenc := findHeaderContains(headers, "Vencimento")

	if idxTitulo < 0 || idxRend < 0 || idxPreco < 0 || idxVenc < 0 {
		return nil, fmt.Errorf("não achei colunas esperadas. headers=%v", headers)
	}

	var out []ResgateRow
	for _, rec := range all[1:] {
		if len(rec) == 0 {
			continue
		}
		if max(idxTitulo, idxRend, idxPreco, idxVenc) >= len(rec) {
			continue
		}

		titulo := strings.TrimSpace(rec[idxTitulo])
		rend := strings.TrimSpace(rec[idxRend])
		rawPreco := strings.TrimSpace(rec[idxPreco])
		venc := strings.TrimSpace(rec[idxVenc])

		if titulo == "" {
			continue
		}

		preco, err := parseBRL(rawPreco)
		if err != nil {
			preco = 0
		}

		out = append(out, ResgateRow{
			Titulo:           titulo,
			RendimentoAnual:  rend,
			PrecoResgate:     preco,
			VencimentoTitulo: venc,
			RawPrecoResgate:  rawPreco,
		})
	}
	return out, nil
}

func findHeader(headers []string, want string) int {
	for i, h := range headers {
		if strings.TrimSpace(h) == want {
			return i
		}
	}
	return -1
}

func findHeaderContains(headers []string, substr string) int {
	substr = strings.ToLower(substr)
	for i, h := range headers {
		if strings.Contains(strings.ToLower(strings.TrimSpace(h)), substr) {
			return i
		}
	}
	return -1
}

// ===== Parsing: money, dates, yield =====

func parseBRL(s string) (float64, error) {
	// Ex.: "R$ 1.234,56" ou "1.234,56"
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "R$", "")
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, ".", "")  // milhar
	s = strings.ReplaceAll(s, ",", ".") // decimal
	return strconv.ParseFloat(s, 64)
}

func parsePtBrDateToYMD(s string) string {
	s = strings.TrimSpace(s)

	// dd/mm/yyyy
	if strings.Count(s, "/") == 2 {
		parts := strings.Split(s, "/")
		if len(parts) == 3 && len(parts[2]) == 4 {
			dd, mm, yyyy := parts[0], parts[1], parts[2]
			if len(dd) == 2 && len(mm) == 2 {
				return fmt.Sprintf("%s-%s-%s", yyyy, mm, dd)
			}
		}
	}

	// yyyy-mm-dd (já pronto)
	if strings.Count(s, "-") == 2 {
		parts := strings.Split(s, "-")
		if len(parts) == 3 && len(parts[0]) == 4 {
			return s
		}
	}
	return ""
}

// Extrai o primeiro percentual e devolve em decimal (ex.: "3,53%" => 0.0353)
func parseYieldPercentToDecimal(s string) float64 {
	s = strings.TrimSpace(s)

	// pega o primeiro número antes do %
	re := regexp.MustCompile(`([0-9]+(?:,[0-9]+)?|[0-9]+(?:\.[0-9]+)?)\s*%`)
	m := re.FindStringSubmatch(s)
	if len(m) < 2 {
		return 0
	}

	num := strings.ReplaceAll(m[1], ".", "") // se vier "1.234,56%" (raro)
	num = strings.ReplaceAll(num, ",", ".")
	f, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0
	}
	return f // / 100.0 (ignoraremos a divisão por enquanto)
}

// ===== Mapping: título -> ticker base =====
// Lógica espelhada do seu script python (com adaptações para o nome vindo do TD CSV).
func inferTickerBaseFromTituloTD(titulo string) string {
	t := strings.ToLower(strings.TrimSpace(titulo))

	// Selic
	if strings.Contains(t, "selic") {
		return "LFT"
	}

	// Prefixados
	if strings.Contains(t, "prefixado") && strings.Contains(t, "juros") {
		return "NTN-F"
	}
	if strings.Contains(t, "prefixado") {
		return "LTN"
	}

	// IPCA
	if strings.Contains(t, "ipca") && strings.Contains(t, "juros") {
		return "NTN-B"
	}
	if strings.Contains(t, "ipca") {
		return "NTN-B P"
	}

	// Outros
	if strings.Contains(t, "igpm") && strings.Contains(t, "juros") {
		return "NTN-C"
	}
	if strings.Contains(t, "renda+") {
		return "NTN-B1 R+"
	}
	if strings.Contains(t, "educa+") || strings.Contains(t, "educa") {
		return "NTN-B1 E+"
	}

	return "TD"
}

func max(nums ...int) int {
	m := nums[0]
	for _, n := range nums[1:] {
		if n > m {
			m = n
		}
	}
	return m
}
