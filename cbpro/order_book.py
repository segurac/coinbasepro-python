#
# cbpro/order_book.py
# David Caseria
#
# Live order book updated from the Coinbase Websocket Feed

from sortedcontainers import SortedDict
from decimal import Decimal
import pickle

from cbpro.public_client import PublicClient
from cbpro.websocket_client import WebsocketClient


class OrderBook(WebsocketClient):
    def __init__(self, product_id='BTC-USD', log_to=None):
        super(OrderBook, self).__init__(
            products=product_id, channels=['full'])
        self._asks = SortedDict()
        self._bids = SortedDict()
        self._client = PublicClient()
        self._sequence = -1
        self._log_to = log_to
        if self._log_to:
            assert hasattr(self._log_to, 'write')
        self._current_ticker = None
        self.price_dict = {}

    @property
    def product_id(self):
        ''' Currently OrderBook only supports a single product even though it is stored as a list of products. '''
        return self.products[0]

    def on_open(self):
        self._sequence = -1
        print("-- Subscribed to OrderBook! --\n")

    def on_close(self):
        print("\n-- OrderBook Socket Closed! --")

    def reset_book(self):
        self._asks = SortedDict()
        self._bids = SortedDict()
        res = self._client.get_product_order_book(product_id=self.product_id, level=3)
        for bid in res['bids']:
            self.add({
                'id': bid[2],
                'side': 'buy',
                'price': Decimal(bid[0]),
                'size': Decimal(bid[1])
            })
        for ask in res['asks']:
            self.add({
                'id': ask[2],
                'side': 'sell',
                'price': Decimal(ask[0]),
                'size': Decimal(ask[1])
            })
        self._sequence = res['sequence']
        self.populate_price_dict()
        
    def populate_price_dict(self):
        self.price_dict = {}
        for asks in self._asks.items():
            for ask in asks[1]:
                self.price_dict[ask['id']] = ask['price']
    
        for bids in self._bids.items():
            for bid in bids[1]:
                self.price_dict[bid['id']] = bid['price']


    def on_message(self, message):
        if self._log_to:
            pickle.dump(message, self._log_to)

        sequence = message.get('sequence', -1)
        if self._sequence == -1:
            self.reset_book()
            return
        if sequence <= self._sequence:
            # ignore older messages (e.g. before order book initialization from getProductOrderBook)
            return
        elif sequence > self._sequence + 1:
            self.on_sequence_gap(self._sequence, sequence)
            return

        msg_type = message['type']
        if msg_type == 'open':
            self.add(message)
        elif msg_type == 'done' and 'price' in message:
            self.remove(message)
            #if not self.remove(message):
                #if message["reason"] == "filled":
                    #self.remove_order_by_id(message)

        elif msg_type == 'match':
            self.match(message)
            self._current_ticker = message
        elif msg_type == 'change':
            self.change(message)

        self._sequence = sequence

    def on_sequence_gap(self, gap_start, gap_end):
        self.reset_book()
        print('Error: messages missing ({} - {}). Re-initializing  book at sequence.'.format(
            gap_start, gap_end, self._sequence))


    def add(self, order):
        order = {
            'id': order.get('order_id') or order['id'],
            'side': order['side'],
            'price': Decimal(order['price']),
            'size': Decimal(order.get('size') or order['remaining_size'])
        }
        if order['side'] == 'buy':
            bids = self.get_bids(order['price'])
            if bids is None:
                bids = [order]
            else:
                bids.append(order)
            self.set_bids(order['price'], bids)
        else:
            asks = self.get_asks(order['price'])
            if asks is None:
                asks = [order]
            else:
                asks.append(order)
            self.set_asks(order['price'], asks)
        self.price_dict[order['id']] = order['price']

    def remove(self, order_in):
        #Try to remove the order and return if it was there or not
        order_id = order_in['order_id']
        order = {}
        order['order_id'] = order_in['order_id']
        order['price'] = order_in['price']
        order['side'] = order_in['side']

        
        if order_id in self.price_dict:
            if Decimal(order_in['price']) != self.price_dict[order_id]:
                print("order_id", order_id, "had an open price of", self.price_dict[order_id] , "and a done price of", order_in['price'])
                order['price'] = self.price_dict[order_id]
            del self.price_dict[order_id]
        
        found_order = False
        price = Decimal(order['price'])
        if order['side'] == 'buy':
            bids = self.get_bids(price)
            if bids is not None:
                for o in bids: 
                    if o['id'] == order['order_id']:
                        found_order=True
                        break
                bids = [o for o in bids if o['id'] != order['order_id']]
                if len(bids) > 0:
                    self.set_bids(price, bids)
                else:
                    self.remove_bids(price)
        else:
            asks = self.get_asks(price)
            if asks is not None:
                for o in asks: 
                    if o['id'] == order['order_id']:
                        found_order=True
                        break
                asks = [o for o in asks if o['id'] != order['order_id']]
                if len(asks) > 0:
                    self.set_asks(price, asks)
                else:
                    self.remove_asks(price)
        return found_order

    def match(self, order):
        size = Decimal(order['size'])
        price = Decimal(order['price'])

        if order['side'] == 'buy':
            bids = self.get_bids(price)
            if not bids:
                return
            if bids[0]['id'] != order['maker_order_id']:
                print(order)
                print("This order is not in the correct position in the orderbook, this may happen after a change order", "maker_order_id", order['maker_order_id'], "first bid", bids[0]['id'])
                new_bids = []
                other_bids = []
                for bid in bids:
                    if bid['id'] == order['maker_order_id']:
                        new_bids.append(bid)
                    else:
                        other_bids.append(bid)
                assert len(new_bids) == 1
                bids = new_bids + other_bids
            assert bids[0]['id'] == order['maker_order_id']

            #Do not remove the order from orderbook, we will remove it later with the done message
#            if bids[0]['size'] == size:
#                self.set_bids(price, bids[1:])
#            else:

            bids[0]['size'] -= size
            self.set_bids(price, bids)
        else:
            asks = self.get_asks(price)
            if not asks:
                return
            if asks[0]['id'] != order['maker_order_id']:
                print(order)
                print("This order is not in the correct position in the orderbook, this may happen after a change order", "maker_order_id", order['maker_order_id'], "first ask", asks[0]['id'])
                new_asks = []
                other_asks = []
                for ask in asks:
                    if ask['id'] == order['maker_order_id']:
                        new_asks.append(ask)
                    else:
                        other_asks.append(ask)
                assert len(new_asks) == 1
                asks = new_asks + other_asks
            assert asks[0]['id'] == order['maker_order_id']
#            if asks[0]['size'] == size:
#                self.set_asks(price, asks[1:])
#            else:
            asks[0]['size'] -= size
            self.set_asks(price, asks)

    def change(self, order):
        try:
            new_size = Decimal(order['new_size'])
        except KeyError:
            print("new_size not in order", order)
            return
        
        
        if 'new_price' in order:
            ##This is not needed because we already take care in add and remove functions
            ##self.price_dict[ order['order_id']  ] = Decimal(order['new_price'])
            #we need to delete old order a5 old_price and create a new one with new_price
            #TODO TODO
            remove_order = {}
            remove_order['price'] = order['old_price']
            remove_order['size'] = order['new_size']
            remove_order['side'] = order['side']
            remove_order['order_id'] = order['order_id']
            if self.remove(remove_order):
                #We should only do the change order if the order was already open, otherwise we will put not-yet open orders in the orderboork, which is wrong
                add_order = {}
                add_order['price'] = order['new_price']
                add_order['size'] = order['new_size']
                add_order['side'] = order['side']
                add_order['order_id'] = order['order_id']
                self.add(add_order)
            else:
                print("ignoring change order", order['order_id'])
            
        else:
            #We just need to change the size of the order at price
            price = None
            if 'price' in order:
                try:
                    price = Decimal(order['price'])
                except:
                    price = None

            print("Will try to find order by id", order['order_id'])
            if order['side'] == 'buy':
                old_order = self.get_bid_by_id( order['order_id'] )
            if order['side'] == 'sell':
                old_order = self.get_ask_by_id( order['order_id'] )
            if old_order is None:
                print("Old order not found, probably the order got changed and then matched another order, not good")
                return
            else:
                if price is None:
                    price = old_order['price']
                    print("Fixed missing price in change order")
                else:
                    assert price == old_order['price']

            if order['side'] == 'buy':
                bids = self.get_bids(price)
                if bids is None or not any(o['id'] == order['order_id'] for o in bids):
                    print("I don't know why I am here, but this is not good")
                    return
                index = [b['id'] for b in bids].index(order['order_id'])
                bids[index]['size'] = new_size
                self.set_bids(price, bids)
            else:
                asks = self.get_asks(price)
                if asks is None or not any(o['id'] == order['order_id'] for o in asks):
                    print("I don't know why I am here, but this is not good")
                    return
                index = [a['id'] for a in asks].index(order['order_id'])
                asks[index]['size'] = new_size
                self.set_asks(price, asks)

        #tree = self._asks if order['side'] == 'sell' else self._bids
        #node = tree.get(price)

        #if node is None or not any(o['id'] == order['order_id'] for o in node):
            #return

    def get_current_ticker(self):
        return self._current_ticker

    def get_current_book(self):
        result = {
            'sequence': self._sequence,
            'asks': [],
            'bids': [],
        }
        for ask in self._asks:
            try:
                # There can be a race condition here, where a price point is removed
                # between these two ops
                this_ask = self._asks[ask]
            except KeyError:
                continue
            for order in this_ask:
                result['asks'].append([order['price'], order['size'], order['id']])
        for bid in self._bids:
            try:
                # There can be a race condition here, where a price point is removed
                # between these two ops
                this_bid = self._bids[bid]
            except KeyError:
                continue

            for order in this_bid:
                result['bids'].append([order['price'], order['size'], order['id']])
        return result

    def get_ask(self):
        return self._asks.peekitem(0)[0]

    def get_asks(self, price):
        return self._asks.get(price)

    def remove_asks(self, price):
        del self._asks[price]

    def set_asks(self, price, asks):
        self._asks[price] = asks

    def get_bid(self):
        return self._bids.peekitem(-1)[0]

    def get_bids(self, price):
        return self._bids.get(price)

    def remove_bids(self, price):
        del self._bids[price]

    def set_bids(self, price, bids):
        self._bids[price] = bids

    def get_ask_by_id(self, id):
        for asks in self._asks.items():
            for ask in asks[1]:
                if ask['id'] == id:
                    return ask
        return None
    
    def get_bid_by_id(self, id):
        for bids in self._bids.items():
            for bid in bids[1]:
                if bid['id'] == id:
                    return bid
        return None
    
    def remove_order_by_id(self, order):
        if order['side'] == 'buy':
            found = self.get_bid_by_id(order['order_id'])
        else:
            found = self.get_ask_by_id(order['order_id'])
        if found:
            print("Found this order in the orderbook with a different price", order)
            remove_order = {}
            remove_order['price'] = found['price']
            remove_order['side'] = order['side']
            remove_order['order_id'] = order['order_id']
            
            self.remove(remove_order)


if __name__ == '__main__':
    import sys
    import time
    import datetime as dt


    class OrderBookConsole(OrderBook):
        ''' Logs real-time changes to the bid-ask spread to the console '''

        def __init__(self, product_id=None):
            super(OrderBookConsole, self).__init__(product_id=product_id)

            # latest values of bid-ask spread
            self._bid = None
            self._ask = None
            self._bid_depth = None
            self._ask_depth = None

        def on_message(self, message):
            super(OrderBookConsole, self).on_message(message)

            # Calculate newest bid-ask spread
            bid = self.get_bid()
            bids = self.get_bids(bid)
            bid_depth = sum([b['size'] for b in bids])
            ask = self.get_ask()
            asks = self.get_asks(ask)
            ask_depth = sum([a['size'] for a in asks])

            if self._bid == bid and self._ask == ask and self._bid_depth == bid_depth and self._ask_depth == ask_depth:
                # If there are no changes to the bid-ask spread since the last update, no need to print
                pass
            else:
                # If there are differences, update the cache
                self._bid = bid
                self._ask = ask
                self._bid_depth = bid_depth
                self._ask_depth = ask_depth
                print('{} {} bid: {:.3f} @ {:.2f}\task: {:.3f} @ {:.2f}'.format(
                    dt.datetime.now(), self.product_id, bid_depth, bid, ask_depth, ask))

    order_book = OrderBookConsole()
    order_book.start()
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        order_book.close()

    if order_book.error:
        sys.exit(1)
    else:
        sys.exit(0)
