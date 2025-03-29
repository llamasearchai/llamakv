"""
Publish/Subscribe implementation for LlamaKV
"""
import logging
import threading
import time
from typing import Dict, List, Set, Callable, Any, Optional, Iterator, Union

from llamakv.exceptions import PubSubError

logger = logging.getLogger(__name__)


class PubSub:
    """
    Publish/Subscribe class for message passing

    Provides a way to subscribe to channels and receive messages.
    """
    def __init__(self, backend):
        """
        Initialize a PubSub with the backend

        Args:
            backend: Backend instance
        """
        self.backend = backend
        self.subscribed_channels = set()
        self.subscribed_patterns = set()
        self.running = False
        self.thread = None
        self.message_callback = None
        self.messages = []
        self.lock = threading.Lock()
    
    def subscribe(self, *channels: str) -> None:
        """
        Subscribe to channels

        Args:
            *channels: Channels to subscribe to
        """
        with self.lock:
            for channel in channels:
                if channel not in self.subscribed_channels:
                    self.backend.subscribe(channel)
                    self.subscribed_channels.add(channel)
                    logger.debug(f"Subscribed to channel: {channel}")
    
    def psubscribe(self, *patterns: str) -> None:
        """
        Subscribe to channel patterns

        Args:
            *patterns: Channel patterns to subscribe to
        """
        with self.lock:
            for pattern in patterns:
                if pattern not in self.subscribed_patterns:
                    self.backend.psubscribe(pattern)
                    self.subscribed_patterns.add(pattern)
                    logger.debug(f"Subscribed to pattern: {pattern}")
    
    def unsubscribe(self, *channels: str) -> None:
        """
        Unsubscribe from channels

        Args:
            *channels: Channels to unsubscribe from
        """
        with self.lock:
            for channel in channels:
                if channel in self.subscribed_channels:
                    self.backend.unsubscribe(channel)
                    self.subscribed_channels.remove(channel)
                    logger.debug(f"Unsubscribed from channel: {channel}")
    
    def punsubscribe(self, *patterns: str) -> None:
        """
        Unsubscribe from channel patterns

        Args:
            *patterns: Channel patterns to unsubscribe from
        """
        with self.lock:
            for pattern in patterns:
                if pattern in self.subscribed_patterns:
                    self.backend.punsubscribe(pattern)
                    self.subscribed_patterns.remove(pattern)
                    logger.debug(f"Unsubscribed from pattern: {pattern}")
    
    def listen(self) -> Iterator[Dict[str, Any]]:
        """
        Listen for messages

        Yields:
            Dict containing message information (type, channel, data)
        """
        if not (self.subscribed_channels or self.subscribed_patterns):
            raise PubSubError("No channels or patterns subscribed")
        
        self.running = True
        
        try:
            # Start a separate thread to receive messages if we're not already
            # listening (to avoid blocking)
            if not self.thread:
                self._start_listening_thread()
            
            # Yield messages as they arrive
            while self.running:
                message = self._get_next_message()
                if message:
                    yield message
                else:
                    # No message available, sleep briefly to avoid busy waiting
                    time.sleep(0.01)
        finally:
            self.running = False
    
    def on_message(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """
        Set a callback for when messages are received

        Args:
            callback: Function to call with the message
        """
        self.message_callback = callback
        
        # Start listening if we're not already
        if not self.thread:
            self._start_listening_thread()
    
    def stop(self) -> None:
        """
        Stop listening for messages
        """
        self.running = False
        if self.thread:
            self.thread.join(timeout=1.0)
            self.thread = None
    
    def _start_listening_thread(self) -> None:
        """
        Start a thread to listen for messages
        """
        self.thread = threading.Thread(target=self._listen_for_messages)
        self.thread.daemon = True
        self.thread.start()
    
    def _listen_for_messages(self) -> None:
        """
        Listen for messages from the backend
        """
        logger.debug("Starting message listener thread")
        
        try:
            while self.running:
                message = self.backend.get_message()
                if message:
                    with self.lock:
                        self.messages.append(message)
                    
                    # Call the callback if set
                    if self.message_callback:
                        try:
                            self.message_callback(message)
                        except Exception as e:
                            logger.error(f"Error in message callback: {e}")
                else:
                    # No message available, sleep briefly to avoid busy waiting
                    time.sleep(0.01)
        except Exception as e:
            logger.error(f"Error in message listener thread: {e}")
        finally:
            logger.debug("Message listener thread stopped")
    
    def _get_next_message(self) -> Optional[Dict[str, Any]]:
        """
        Get the next message from the queue

        Returns:
            Dict containing message information or None if no message is available
        """
        with self.lock:
            if self.messages:
                return self.messages.pop(0)
        return None
    
    def __enter__(self) -> 'PubSub':
        """
        Enter context manager

        Returns:
            self
        """
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Exit context manager

        Stops listening for messages.
        """
        self.stop() 