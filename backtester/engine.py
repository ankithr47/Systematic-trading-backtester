from queue import Queue, Empty
from backtester.events import EventType

class BacktestEnging:
    #Orchestrates event driven backtest
    #Owns the event loop and time progression

    def __init__(self, data_handler, strategy, portfolio, execution_handler):
        self.events = Queue()
        self.data_handler = data_handler
        self.strategy = strategy
        self.portfolio = portfolio
        self.execution_handler = execution_handler

        #Inject shared event queue
        self.data_handler.events = self.events
        self.strategy.events = self.events
        self.portfolio.events = self.events
        self.execution_handler.events = self.events
    
    def run(self):
        #Main event driven loop
        while self.data_handler.has_more_data():
            #advance market by one step
            self.data_handler.update_bars()

            #process all resulting events
            while True:
                try:
                    event = self.events.get(False)
                except Empty:
                    break
                
                if event.type == EventType.MARKET:
                    self.portfolio.update_timeindex()
                    self.strategy.calculate_signals()
                
                elif event.type == EventType.SIGNAL:
                    self.portfolio.generate_order(event)
                
                elif event.type == EventType.ORDER:
                    self.execution_handler.execute_order(event)
                
                elif event.type == EventType.FILL:
                    self.portfolio.update_fill(event)
        
        return self.portfolio.history_dataframe()