from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.model import InstrumentId, TraderId
from nautilus_trader.model.data import DataType
from nautilus_trader.core import Data
from nautilus_trader.config import StrategyConfig, BacktestEngineConfig, LoggingConfig
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.model.objects import Money
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import AccountType, OmsType
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.test_kit.providers import TestInstrumentProvider

import pyarrow as pa
import msgspec
from nautilus_trader.model.custom import customdataclass

@customdataclass
class TweetSignalData(Data):
    def __init__(self, instrument_id: InstrumentId, ts_init: int, ts_event: int, signal: int):
        self.instrument_id = instrument_id
        self._ts_init = ts_init
        self._ts_event = ts_event
        self.signal = signal

    @property
    def ts_init(self):
        return self._ts_init
        
    @property
    def ts_event(self):
        return self._ts_event

    def to_dict(self):
        return {
            "instrument_id": self.instrument_id.value,
            "ts_init": self._ts_init,
            "ts_event": self._ts_event,
            "signal": self.signal,
        }

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            instrument_id=InstrumentId.from_str(data["instrument_id"]),
            ts_init=data["ts_init"],
            ts_event=data["ts_event"],
            signal=data["signal"]
        )

    def to_bytes(self):
        return msgspec.msgpack.encode(self.to_dict())

    @classmethod
    def from_bytes(cls, data: bytes):
        return cls.from_dict(msgspec.msgpack.decode(data))

    @classmethod
    def schema(cls):
        return pa.schema({
            "instrument_id": pa.string(),
            "ts_init": pa.int64(),
            "ts_event": pa.int64(),
            "signal": pa.int64(),
        })


class MyStrategyConfig(StrategyConfig, frozen=True):
    data_type: DataType


class MyStrategy(Strategy):
    def __init__(self, config: MyStrategyConfig):
        super().__init__(config=config)
        self.count_of_tweets = 0

    def on_start(self):
        self.log.info(f"Subscribe custom data: {self.config.data_type}")
        self.subscribe_data(self.config.data_type)

    def on_data(self, data: Data):
        if isinstance(data, TweetSignalData):
            self.count_of_tweets += 1
            self.log.info(f"Received tweet signal: {data.signal}")
            print(f"Received tweet signal: {self.count_of_tweets}")

    def on_end(self):
        self.log.info(f"Total tweet: {self.count_of_tweets}")


engine = BacktestEngine(
    config = BacktestEngineConfig(
        trader_id = TraderId("BACKTEST-TWEET-001"),
        logging = LoggingConfig(log_level = "DEBUG")
        )
)

# Venue and Instrument
venue = Venue("XCME")
engine.add_venue(
    venue=venue,
    oms_type=OmsType.NETTING,
    account_type=AccountType.MARGIN,
    starting_balances=[Money(1_000_000, USD)],
    base_currency=USD,
)

instrument = TestInstrumentProvider.eurusd_future(2024, 3, "XCME")
engine.add_instrument(instrument)

# Data
tweet_data = [
    TweetSignalData(instrument.id, 1704150060000000000, 1704150060000000000, 1),
    TweetSignalData(instrument.id, 1704150120000000000, 1704150120000000000, 0),
]
engine.add_data(tweet_data)

# Strategy
engine.add_strategy(MyStrategy(config=MyStrategyConfig(
    data_type = DataType(TweetSignalData)
)))

# Run
engine.run()

engine.dispose()
