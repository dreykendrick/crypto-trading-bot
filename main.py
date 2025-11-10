"""
AI-Powered Crypto Trading Bot - Main Orchestrator
Educational framework for algorithmic trading on Binance
"""

import asyncio
import logging
import signal
import sys
from datetime import datetime
from typing import Optional

from strategy import StrategyManager
from risk_manager import RiskManager
from data_loader import DataLoader
from backtester import Backtester
import json
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/trading_bot_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class TradingBot:
    """Main trading bot orchestrator"""
    
    def __init__(self, config_path: str = 'config.json'):
        """Initialize trading bot with configuration"""
        self.config = self._load_config(config_path)
        self.running = False
        self.mode = self.config.get('mode', 'paper')  # paper or live
        
        # Initialize components
        self.data_loader = DataLoader(
            api_key=os.getenv('BINANCE_API_KEY'),
            api_secret=os.getenv('BINANCE_API_SECRET'),
            testnet=self.config.get('testnet', True)
        )
        
        self.risk_manager = RiskManager(
            initial_capital=self.config['risk']['initial_capital'],
            max_position_size=self.config['risk']['max_position_size'],
            max_drawdown=self.config['risk']['max_drawdown'],
            daily_loss_limit=self.config['risk']['daily_loss_limit']
        )
        
        self.strategy_manager = StrategyManager(
            strategy_name=self.config['strategy']['name'],
            params=self.config['strategy']['params']
        )
        
        # Trading state
        self.positions = {}
        self.equity_curve = []
        self.trade_history = []
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            logger.info(f"Configuration loaded from {config_path}")
            return config
        except FileNotFoundError:
            logger.error(f"Config file {config_path} not found")
            sys.exit(1)
    
    async def run_backtest(self):
        """Run backtesting mode"""
        logger.info("=" * 60)
        logger.info("Starting Backtesting Mode")
        logger.info("=" * 60)
        
        symbol = self.config['trading']['symbol']
        interval = self.config['trading']['interval']
        
        # Load historical data
        historical_data = await self.data_loader.get_historical_data(
            symbol=symbol,
            interval=interval,
            lookback_days=self.config['backtest']['lookback_days']
        )
        
        # Initialize backtester
        backtester = Backtester(
            strategy=self.strategy_manager,
            risk_manager=self.risk_manager,
            initial_capital=self.config['risk']['initial_capital'],
            commission=self.config['trading']['commission']
        )
        
        # Run backtest
        results = await backtester.run(historical_data, symbol)
        
        # Display results
        backtester.print_results(results)
        
        # Save results
        self._save_backtest_results(results)
        
        logger.info("Backtesting completed")
        
    async def run_paper_trading(self):
        """Run paper trading mode (simulated live trading)"""
        logger.info("=" * 60)
        logger.info("Starting Paper Trading Mode")
        logger.info("=" * 60)
        
        self.running = True
        symbol = self.config['trading']['symbol']
        interval = self.config['trading']['interval']
        
        # Connect to live data stream
        await self.data_loader.connect_websocket(symbol)
        
        try:
            while self.running:
                # Get latest market data
                current_data = await self.data_loader.get_latest_data(symbol, interval)
                
                if current_data is None:
                    await asyncio.sleep(1)
                    continue
                
                # Generate signals
                signal = self.strategy_manager.generate_signal(current_data)
                
                # Check risk constraints
                if not self.risk_manager.can_trade():
                    logger.warning("Risk limits reached. Trading paused.")
                    await asyncio.sleep(60)
                    continue
                
                # Execute trading logic
                await self._execute_signal(symbol, signal, current_data)
                
                # Update positions and equity
                await self._update_positions(symbol, current_data)
                
                # Log status
                self._log_status()
                
                # Wait for next cycle
                await asyncio.sleep(self.config['trading']['check_interval'])
                
        except Exception as e:
            logger.error(f"Error in paper trading: {e}", exc_info=True)
        finally:
            await self.data_loader.close_websocket()
    
    async def run_live_trading(self):
        """Run live trading mode (REAL MONEY - USE WITH EXTREME CAUTION)"""
        logger.warning("=" * 60)
        logger.warning("LIVE TRADING MODE - REAL MONEY AT RISK")
        logger.warning("=" * 60)
        
        # Additional safety check
        confirmation = input("Are you ABSOLUTELY sure you want to trade with real money? (type 'YES' to confirm): ")
        if confirmation != "YES":
            logger.info("Live trading cancelled by user")
            return
        
        logger.warning("Starting live trading in 10 seconds... Press Ctrl+C to cancel")
        await asyncio.sleep(10)
        
        # Similar logic to paper trading but with real order execution
        logger.info("Live trading started")
        # Implementation would be similar to paper trading but calling real order endpoints
        # Left as exercise for safety reasons
        
    async def _execute_signal(self, symbol: str, signal: dict, market_data: dict):
        """Execute trading signal"""
        if signal['action'] == 'NONE':
            return
        
        current_price = market_data['close'].iloc[-1]
        
        if signal['action'] == 'BUY' and symbol not in self.positions:
            # Calculate position size
            position_size = self.risk_manager.calculate_position_size(
                current_price,
                signal.get('stop_loss', current_price * 0.98)
            )
            
            if position_size > 0:
                # Simulate order execution (paper trading)
                self.positions[symbol] = {
                    'entry_price': current_price,
                    'size': position_size,
                    'entry_time': datetime.now(),
                    'stop_loss': signal.get('stop_loss', current_price * 0.98),
                    'take_profit': signal.get('take_profit', current_price * 1.05)
                }
                
                self.risk_manager.open_position(position_size * current_price)
                
                logger.info(f"BUY Signal Executed: {symbol} @ {current_price:.2f} | Size: {position_size:.4f}")
                
        elif signal['action'] == 'SELL' and symbol in self.positions:
            # Close position
            position = self.positions[symbol]
            profit = (current_price - position['entry_price']) * position['size']
            profit_pct = ((current_price / position['entry_price']) - 1) * 100
            
            self.risk_manager.close_position(profit)
            
            # Record trade
            self.trade_history.append({
                'symbol': symbol,
                'entry_price': position['entry_price'],
                'exit_price': current_price,
                'size': position['size'],
                'profit': profit,
                'profit_pct': profit_pct,
                'entry_time': position['entry_time'],
                'exit_time': datetime.now()
            })
            
            logger.info(f"SELL Signal Executed: {symbol} @ {current_price:.2f} | Profit: ${profit:.2f} ({profit_pct:.2f}%)")
            
            del self.positions[symbol]
    
    async def _update_positions(self, symbol: str, market_data: dict):
        """Update open positions and check stop-loss/take-profit"""
        if symbol not in self.positions:
            return
        
        position = self.positions[symbol]
        current_price = market_data['close'].iloc[-1]
        
        # Check stop-loss
        if current_price <= position['stop_loss']:
            logger.warning(f"Stop-loss triggered for {symbol}")
            await self._execute_signal(symbol, {'action': 'SELL'}, market_data)
            return
        
        # Check take-profit
        if current_price >= position['take_profit']:
            logger.info(f"Take-profit triggered for {symbol}")
            await self._execute_signal(symbol, {'action': 'SELL'}, market_data)
            return
        
        # Update trailing stop if configured
        if self.config['risk'].get('use_trailing_stop', False):
            trailing_pct = self.config['risk'].get('trailing_stop_pct', 0.02)
            new_stop = current_price * (1 - trailing_pct)
            if new_stop > position['stop_loss']:
                position['stop_loss'] = new_stop
    
    def _log_status(self):
        """Log current bot status"""
        equity = self.risk_manager.get_current_equity()
        self.equity_curve.append({
            'timestamp': datetime.now(),
            'equity': equity
        })
        
        if len(self.equity_curve) % 60 == 0:  # Log every 60 cycles
            logger.info(f"Current Equity: ${equity:.2f} | Open Positions: {len(self.positions)} | Total Trades: {len(self.trade_history)}")
    
    def _save_backtest_results(self, results: dict):
        """Save backtest results to file"""
        os.makedirs('results', exist_ok=True)
        filename = f"results/backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info(f"Results saved to {filename}")
    
    def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down trading bot...")
        self.running = False
        
        # Save final state
        self._save_state()
        
        logger.info("Bot shutdown complete")
    
    def _save_state(self):
        """Save bot state for recovery"""
        state = {
            'timestamp': datetime.now().isoformat(),
            'equity_curve': self.equity_curve,
            'trade_history': self.trade_history,
            'positions': self.positions
        }
        
        os.makedirs('data', exist_ok=True)
        with open('data/bot_state.json', 'w') as f:
            json.dump(state, f, indent=2, default=str)


async def main():
    """Main entry point"""
    # Create necessary directories
    os.makedirs('logs', exist_ok=True)
    os.makedirs('data', exist_ok=True)
    os.makedirs('results', exist_ok=True)
    
    # Initialize bot
    bot = TradingBot()
    
    # Setup signal handlers for graceful shutdown
    def signal_handler(sig, frame):
        logger.info("Interrupt received, shutting down...")
        bot.shutdown()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run based on mode
    mode = bot.config.get('mode', 'backtest')
    
    if mode == 'backtest':
        await bot.run_backtest()
    elif mode == 'paper':
        await bot.run_paper_trading()
    elif mode == 'live':
        await bot.run_live_trading()
    else:
        logger.error(f"Unknown mode: {mode}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())