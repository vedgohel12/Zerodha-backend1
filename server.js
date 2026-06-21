// server.js
//
// Live price feed server for the watchlist.
//
// How it works:
//   1. We keep a list of stock symbols (Yahoo Finance format, e.g. "INFY.NS").
//   2. Every POLL_INTERVAL_MS, we fetch fresh quotes for all symbols in one batch call.
//   3. We broadcast the updated prices to every connected Socket.io client.
//   4. The frontend just listens on the "watchlist:update" event and re-renders.
//
// NOTE on data freshness: Yahoo Finance's free, unauthenticated quote data for
// NSE/BSE symbols is not official real-time exchange data — it's typically a
// short delay behind the live tape (commonly ~15 minutes for Indian exchanges,
// sometimes less depending on Yahoo's own feed). For a UI that visibly updates
// in real time (which is what this server gives you), that's a perfectly fine
// starting point. If you need true tick-by-tick real-time data, swap the
// fetchQuotes() function below for a broker WebSocket feed (e.g. Upstox) later
// — the rest of this server (the Socket.io broadcast layer) stays the same.

const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");
const YahooFinance = require("yahoo-finance2").default;

const yahooFinance = new YahooFinance({
  suppressNotices: ["yahooSurvey"],
});

const PORT = process.env.PORT || 5050;
const POLL_INTERVAL_MS = 5000; // how often to refresh prices
// Yeh tumhare React dashboard ka URL hai (jahan WatchList component render hota hai).
const DASHBOARD_ORIGIN = process.env.DASHBOARD_ORIGIN || "http://localhost:3001";

// 🔧 Edit this list to match the symbols in your watchlist.
// Yahoo Finance symbol format for Indian exchanges:
//   NSE -> "<SYMBOL>.NS"   e.g. "INFY.NS", "RELIANCE.NS", "TCS.NS"
//   BSE -> "<SYMBOL>.BO"   e.g. "RELIANCE.BO"
// "displayName" is what gets shown in the UI (matches your existing watchlist.name).
const SYMBOLS = [
  { symbol: "INFY.NS", displayName: "INFY" },
  { symbol: "RELIANCE.NS", displayName: "RELIANCE" },
  { symbol: "TCS.NS", displayName: "TCS" },
  { symbol: "HDFCBANK.NS", displayName: "HDFCBANK" },
  { symbol: "ICICIBANK.NS", displayName: "ICICIBANK" },
  { symbol: "TATAMOTORS.NS", displayName: "TATAMOTORS" },
  { symbol: "WIPRO.NS", displayName: "WIPRO" },
  { symbol: "^NSEI", displayName: "NIFTY 50" },
  { symbol: "^BSESN", displayName: "SENSEX" },
];

const app = express();
app.use(cors({ origin: DASHBOARD_ORIGIN }));
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: DASHBOARD_ORIGIN,
    methods: ["GET", "POST"],
  },
});

// In-memory cache of the latest known prices, so a client connecting mid-cycle
// gets data immediately instead of waiting for the next poll.
let latestWatchlist = [];

async function fetchQuotes() {
  const symbols = SYMBOLS.map((s) => s.symbol);

  try {
    // Fetch quotes one symbol at a time and keep partial results if some fail.
    const quoteResults = await Promise.all(
      symbols.map(async (symbol) => {
        try {
          return await yahooFinance.quote(symbol);
        } catch (error) {
          console.warn(`⚠️ Quote fetch failed for ${symbol}:`, error.message);
          return null;
        }
      })
    );

    const quoteBySymbol = new Map(
      quoteResults
        .filter(Boolean)
        .map((q) => [q.symbol, q])
    );

    latestWatchlist = SYMBOLS.map(({ symbol, displayName }) => {
      const q = quoteBySymbol.get(symbol);

      if (!q) {
        const existing = latestWatchlist.find((w) => w.symbol === symbol);
        return (
          existing || {
            symbol,
            name: displayName,
            price: 0,
            percent: "0.00%",
            isDown: false,
          }
        );
      }

      const price = q.regularMarketPrice ?? 0;
      const changePercent = q.regularMarketChangePercent ?? 0;
      const isDown = changePercent < 0;

      return {
        symbol,
        name: displayName,
        price: Number(price.toFixed(2)),
        percent: `${isDown ? "" : "+"}${changePercent.toFixed(2)}%`,
        isDown,
      };
    });

    io.emit("watchlist:update", latestWatchlist);
    console.log(`🔁 Broadcasted watchlist update (${new Date().toLocaleTimeString()})`);
  } catch (err) {
    console.error("❌ Failed to fetch quotes:", err.message);
  }
}

io.on("connection", (socket) => {
  console.log("🔌 Client connected:", socket.id);

  // Send whatever we already have immediately on connect.
  if (latestWatchlist.length > 0) {
    socket.emit("watchlist:update", latestWatchlist);
  }

  socket.on("disconnect", () => {
    console.log("❌ Client disconnected:", socket.id);
  });
});

app.get("/health", (req, res) => {
  res.json({ status: "ok", symbolCount: SYMBOLS.length, lastUpdate: latestWatchlist });
});

server.listen(PORT, () => {
  console.log(`🚀 Live watchlist server running on http://localhost:${PORT}`);
  fetchQuotes(); // run once immediately on startup
  setInterval(fetchQuotes, POLL_INTERVAL_MS);
});