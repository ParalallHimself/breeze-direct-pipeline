import asyncio
import json
import logging
from typing import List, Dict, Any
from config.settings import settings
from backend.database import get_db_connection

# Configure logger for pipeline visibility
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ingestion")

class TickIngestionPipeline:
    """
    Manages real-time WebSocket tick processing, in-memory buffering,
    and batch database persistence for low-latency streaming.
    """
    def __init__(self, batch_size: int = 50, flush_interval: float = 1.0):
        self.tick_queue: asyncio.Queue = asyncio.Queue()
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.is_running = False

    def on_ticks(self, raw_tick: Dict[str, Any]):
        """
        Synchronous callback triggered by Breeze WebSocket engine.
        Pushes raw tick directly into the in-memory queue.
        """
        try:
            # Safely put tick in thread-safe queue without blocking
            self.tick_queue.put_nowait(raw_tick)
        except Exception as e:
            logger.error(f"Error queueing incoming tick: {e}")

    async def _flush_buffer_to_db(self, buffer: List[Dict[str, Any]]):
        """
        Bulk writes buffered ticks to SQLite in a single transaction.
        """
        if not buffer:
            return

        records = []
        for tick in buffer:
            # Parse common Breeze API tick fields safely
            stock_code = tick.get("stock_code") or tick.get("symbol", "UNKNOWN")
            exchange_code = tick.get("exchange_code", settings.DEFAULT_EXCHANGE)
            last_price = float(tick.get("last", tick.get("lTP", 0.0)))
            
            open_p = float(tick.get("open", 0.0)) if tick.get("open") else None
            high_p = float(tick.get("high", 0.0)) if tick.get("high") else None
            low_p = float(tick.get("low", 0.0)) if tick.get("low") else None
            close_p = float(tick.get("close", 0.0)) if tick.get("close") else None
            
            best_bid_price = float(tick.get("bPrice", 0.0)) if tick.get("bPrice") else None
            best_bid_qty = int(tick.get("bQty", 0)) if tick.get("bQty") else None
            best_ask_price = float(tick.get("sPrice", 0.0)) if tick.get("sPrice") else None
            best_ask_qty = int(tick.get("sQty", 0)) if tick.get("sQty") else None
            
            last_traded_qty = int(tick.get("ltq", 0)) if tick.get("ltq") else None
            total_traded_vol = int(tick.get("ttq", 0)) if tick.get("ttq") else None

            raw_payload = json.dumps(tick)

            records.append((
                stock_code, exchange_code, last_price,
                open_p, high_p, low_p, close_p,
                best_bid_price, best_bid_qty,
                best_ask_price, best_ask_qty,
                last_traded_qty, total_traded_vol,
                raw_payload
            ))

        query = """
            INSERT INTO ticks (
                stock_code, exchange_code, last_price,
                open, high, low, close,
                best_bid_price, best_bid_qty,
                best_ask_price, best_ask_qty,
                last_traded_qty, total_traded_vol,
                raw_payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        try:
            db = await get_db_connection()
            await db.executemany(query, records)
            await db.commit()
            await db.close()
            logger.info(f"Successfully persisted {len(records)} ticks to database.")
        except Exception as e:
            logger.error(f"Failed to batch insert ticks: {e}")

    async def start_batch_worker(self):
        """
        Asynchronous background task that processes queue items in batches.
        Flushes either when buffer hits `batch_size` or every `flush_interval` seconds.
        """
        self.is_running = True
        logger.info("Tick ingestion batch worker started.")
        
        buffer = []
        while self.is_running:
            try:
                # Wait for next item or timeout to flush partial buffer
                try:
                    tick = await asyncio.wait_for(self.tick_queue.get(), timeout=self.flush_interval)
                    buffer.append(tick)
                    self.tick_queue.task_done()
                except asyncio.TimeoutError:
                    pass  # Flush interval reached

                # Trigger flush if threshold reached or timeout elapsed with items
                if len(buffer) >= self.batch_size or (buffer and self.tick_queue.empty()):
                    await self._flush_buffer_to_db(buffer)
                    buffer.clear()

            except Exception as e:
                logger.error(f"Error in batch worker loop: {e}")
                await asyncio.sleep(0.5)

    def stop(self):
        """Signals the batch worker loop to shut down cleanly."""
        self.is_running = False

# Global pipeline worker instance
ingestion_pipeline = TickIngestionPipeline()