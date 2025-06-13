# TradeMateDataFetcher

A lightweight Python client for streaming and processing **B3 Trademate** “Atividades do Dia” via Server-Sent Events (SSE).  
Connects to the Trademate SSE endpoint, collects live trade data, transforms it into a pandas DataFrame (with `DATE` and `HORA` columns), filters by ticker symbol, and optionally saves to CSV.

---

## Features
- **Real-time SSE streaming** from `https://trademate.b3.com.br/api/get-activities`
- **Automatic parsing** of JSON‐encoded SSE lines
- **Date & time formatting** (`DD-MM-YYYY`, `HH:MM:SS,mmm`)
- **Conversion** of B3’s `units` + `nanos` format into decimal values
- **DataFrame output** matching the on-screen table structure:
