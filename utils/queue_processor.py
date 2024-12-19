import asyncio
from typing import Optional, List
import logging
from datetime import datetime
from dataclasses import dataclass
from data_collection.bse_data_collection import get_all_data_from_name

@dataclass(order=True)
class QueueItem:
    stock_name: str
    timestamp: datetime
    priority: int = 1

class AnnouncementQueue:
    def __init__(self):
        self.queue = asyncio.PriorityQueue()
        self.processing = set()  # Track stocks currently being processed
        self._stop = False
        self.logger = logging.getLogger(__name__)

    async def add_item(self, stock_name: str, priority: int = 1) -> bool:
        """
        Add a stock to the queue if it's not already being processed
        
        Args:
            stock_name: The stock symbol to process
            priority: Priority level (lower number = higher priority)
            
        Returns:
            bool: True if item was added, False if already in processing
        """
        if stock_name in self.processing:
            self.logger.info(f"Stock {stock_name} is already being processed")
            return False
            
        item = QueueItem(
            stock_name=stock_name,
            timestamp=datetime.now(),
            priority=priority
        )
        # Put only the item without wrapping it in a tuple
        await self.queue.put(item)
        self.logger.info(f"Added {stock_name} to queue with priority {priority}")
        return True

    async def get_next_item(self) -> Optional[QueueItem]:
        """Get the next item from the queue"""
        try:
            item = await self.queue.get()  # Get the item directly
            self.processing.add(item.stock_name)
            return item
        except asyncio.QueueEmpty:
            return None

    def mark_complete(self, stock_name: str):
        """Mark a stock as done processing"""
        if stock_name in self.processing:
            self.processing.remove(stock_name)
            self.queue.task_done()
            self.logger.info(f"Completed processing {stock_name}")

    def stop(self):
        """Signal the processor to stop"""
        self._stop = True

    @property
    def should_stop(self) -> bool:
        return self._stop

# Global queue instance
announcement_queue = AnnouncementQueue()

async def queue_processor(batch_size: int = 5):
    """
    Process items from the queue in batches
    
    Args:
        batch_size: Number of stocks to process concurrently
    """
    from data_collection.bse_data_collection import get_all_data_from_name
    
    while not announcement_queue.should_stop:
        try:
            # Process up to batch_size items concurrently
            batch = []
            for _ in range(batch_size):
                item = await announcement_queue.get_next_item()
                if item:
                    batch.append(item)
                if len(batch) >= batch_size:
                    break

            if not batch:
                await asyncio.sleep(1)  # Wait if queue is empty
                continue

            # Process batch concurrently
            tasks = []
            for item in batch:
                task = asyncio.create_task(process_stock(item))
                tasks.append(task)
            
            await asyncio.gather(*tasks)

        except Exception as e:
            logging.error(f"Error in queue processor: {e}")
            await asyncio.sleep(1)

async def process_stock(item: QueueItem):
    """Process a single stock from the queue"""
    try:
        # Your existing processing logic
        get_all_data_from_name(item.stock_name)
    except Exception as e:
        logging.error(f"Error processing {item.stock_name}: {e}")
    finally:
        announcement_queue.mark_complete(item.stock_name)