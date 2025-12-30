import json
from websockets.client import connect as connect_async
from lighter.configuration import Configuration

class QueueWsClient:
    def __init__(
        self,
        host=None,
        path="/stream",
        order_book_ids=[],
        account_ids=[],
        queue=None,
        auth_token=None,
    ):
        if host is None:
            host = Configuration.get_default().host.replace("https://", "")

        self.base_url = f"wss://{host}{path}"

        self.subscriptions = {
            "order_books": order_book_ids,
            "account_all_orders": account_ids,
            "account_all_trades": account_ids,
        }

        if len(order_book_ids) == 0 and len(account_ids) == 0:
            raise Exception("No subscriptions provided.")

        self.order_book_states = {}
        self.mid_prices = {}
        self.account_states = {}

        self.queue = queue
        self.auth_token = auth_token

        self.ws = None
        self._stop_event = None
    
    def update_auth_token(self, new_token):
        """Update the auth token. Called periodically by Engine."""
        self.auth_token = new_token

    async def on_message_async(self, ws, message):
        if isinstance(message, str):
            message = json.loads(message)

        message_type = message.get("type")

        if message_type == "connected":
            await self.handle_connected_async(ws)
        elif message_type == "subscribed/order_book":
            self.handle_subscribed_order_book(message)
        elif message_type == "update/order_book":
            self.handle_update_order_book(message)
        elif message_type == "subscribed/account_all_orders":
            self.handle_subscribed_account_all_orders(message)
        elif message_type == "update/account_all_orders":
            self.handle_update_account_all_orders(message)
        elif message_type == "subscribed/account_all_trades":
            self.handle_subscribed_account_all_trades(message)
        elif message_type == "update/account_all_trades":
            self.handle_update_account_all_trades(message)
        elif message_type == "ping":
            # Respond to ping with pong
            await ws.send(json.dumps({"type": "pong"}))
        else:
            self.handle_unhandled_message(message)


    async def handle_connected_async(self, ws):
        for market_id in self.subscriptions["order_books"]:
            await ws.send(
                json.dumps({"type": "subscribe", "channel": f"order_book/{market_id}"})
            )
        
        if self.auth_token:
            for account_id in self.subscriptions["account_all_orders"]:
                await ws.send(
                    json.dumps({
                        "type": "subscribe",
                        "channel": f"account_all_orders/{account_id}",
                        "auth": self.auth_token
                    })
                )

            for account_id in self.subscriptions["account_all_trades"]:
                await ws.send(
                    json.dumps({
                        "type": "subscribe",
                        "channel": f"account_all_trades/{account_id}",
                        "auth": self.auth_token
                    })
                )

    def handle_subscribed_order_book(self, message):
        market_id = message["channel"].split(":")[1]
        self.order_book_states[market_id] = message["order_book"]
        
        mid_price = self.calculate_mid_price(market_id)
        # Only queue if price is valid (> 0)
        if mid_price > 0:
            self.mid_prices[market_id] = mid_price
            if self.queue:
                self.queue.put_nowait(("mid_price", market_id, mid_price))

    def handle_update_order_book(self, message):
        market_id = message["channel"].split(":")[1]
        self.update_order_book_state(market_id, message["order_book"])
        
        new_mid_price = self.calculate_mid_price(market_id)
        old_mid_price = self.mid_prices.get(market_id)
        
        # Only queue if price is valid (> 0) and has changed
        if new_mid_price > 0 and new_mid_price != old_mid_price:
            self.mid_prices[market_id] = new_mid_price
            if self.queue:
                self.queue.put_nowait(("mid_price", market_id, new_mid_price))

    def calculate_mid_price(self, market_id):
        order_book = self.order_book_states.get(market_id)
        if not order_book:
            return 0.0

        bids = order_book.get("bids", [])
        asks = order_book.get("asks", [])

        best_bid = 0.0
        if bids:
            best_bid = max(float(bid["price"]) for bid in bids)
        
        best_ask = 0.0
        if asks:
            best_ask = min(float(ask["price"]) for ask in asks)

        if best_bid > 0 and best_ask > 0:
            return (best_bid + best_ask) / 2.0
        elif best_bid > 0:
            return best_bid
        elif best_ask > 0:
            return best_ask
        return 0.0

    def update_order_book_state(self, market_id, order_book):
        self.update_orders(
            order_book["asks"], self.order_book_states[market_id]["asks"]
        )
        self.update_orders(
            order_book["bids"], self.order_book_states[market_id]["bids"]
        )

    def update_orders(self, new_orders, existing_orders):
        for new_order in new_orders:
            is_new_order = True
            for existing_order in existing_orders:
                if new_order["price"] == existing_order["price"]:
                    is_new_order = False
                    existing_order["size"] = new_order["size"]
                    if float(new_order["size"]) == 0:
                        existing_orders.remove(existing_order)
                    break
            if is_new_order:
                existing_orders.append(new_order)

        existing_orders = [
            order for order in existing_orders if float(order["size"]) > 0
        ]

    
    def handle_subscribed_account_all_orders(self, message):
        """Handle initial orders snapshot from account_all_orders channel"""
        account_id = message["channel"].split(":")[1]
        if self.queue:
            self.queue.put_nowait(("open_orders", account_id, message))
    
    def handle_update_account_all_orders(self, message):
        """Handle order updates from account_all_orders channel"""
        account_id = message["channel"].split(":")[1]
        if self.queue:
            self.queue.put_nowait(("open_orders", account_id, message))
    
    def handle_subscribed_account_all_trades(self, message):
        """Handle initial trades snapshot from account_all_trades channel"""
        account_id = message["channel"].split(":")[1]
        if self.queue:
            self.queue.put_nowait(("user_fills", account_id, message))
    
    def handle_update_account_all_trades(self, message):
        """Handle trade updates from account_all_trades channel"""
        account_id = message["channel"].split(":")[1]
        if self.queue:
            self.queue.put_nowait(("user_fills", account_id, message))

    def handle_unhandled_message(self, message):
        raise Exception(f"Unhandled message: {message}")

    def on_error(self, ws, error):
        raise Exception(f"Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        raise Exception(f"Closed: {close_status_code} {close_msg}")

    def stop(self):
        if self._stop_event:
            self._stop_event.set()
        if self.ws:
            # We use loop.create_task if we are in an async loop
            # and the ws is an async client.
            try:
                import asyncio
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self.ws.close())
            except:
                # Fallback if no loop is running or other issues
                pass

    async def run_async(self):
        import asyncio
        self._stop_event = asyncio.Event()
        while not self._stop_event.is_set():
            try:
                ws = await connect_async(self.base_url)
                self.ws = ws
                async for message in ws:
                    if self._stop_event.is_set():
                        break
                    await self.on_message_async(ws, message)
            except Exception as e:
                if self._stop_event.is_set():
                    break
                print(f"WS Connection failed/closed: {e}. Reconnecting in 5s...")
                try:
                    await asyncio.sleep(5)
                except asyncio.CancelledError:
                    break
        
        if self.ws:
            try:
                await self.ws.close()
            except:
                pass
