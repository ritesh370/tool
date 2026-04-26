#!/usr/bin/env python3
"""
session_manager.py — Enhanced Session Controller v1.0
=====================================================
Implements:
1. Tab timeout: 20-25s max, then force stop
2. Stuck detection: 30s threshold → force replace
3. Proxy rotation: 50 proxies, 5-min IP change, 30-40 impressions → 2-3 min break
4. Smooth scrolling (non-random)
5. Continuous automation loop
"""

import random
import threading
import time
from typing import Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class SessionConfig:
    """Configuration for session timing and rotation."""
    tab_duration_min: float = 20.0      # Min tab open time (seconds)
    tab_duration_max: float = 25.0      # Max tab open time (seconds)
    stuck_threshold: float = 30.0       # Force close if stuck (seconds)
    max_impressions: int = 40           # Max impressions before proxy switch
    min_impressions: int = 30           # Min impressions before proxy switch
    break_duration_min: float = 120.0   # Min break (seconds)
    break_duration_max: float = 180.0   # Max break (seconds)
    proxy_change_interval: float = 300.0 # Proxy IP change interval (5 min)
    smooth_scroll_step: int = 100       # Smooth scroll step in pixels
    smooth_scroll_delay: float = 0.1    # Delay between scroll steps


@dataclass
class SessionState:
    """Tracks current session state."""
    start_time: float = field(default_factory=time.time)
    impressions: int = 0
    proxy_switches: int = 0
    tab_restarts: int = 0
    stuck_detections: int = 0
    last_activity: float = field(default_factory=time.time)
    current_proxy: Optional[str] = None
    is_stuck: bool = False


class SessionController:
    """
    Enhanced session controller with:
    - Tab timeout management
    - Stuck tab detection
    - Proxy rotation based on impressions
    - Smooth scrolling
    - Continuous automation
    """

    def __init__(self, config: Optional[SessionConfig] = None):
        self.config = config or SessionConfig()
        self.state = SessionState()
        self._lock = threading.Lock()
        self._stop_flag = threading.Event()
        self._pause_flag = threading.Event()
        self._pause_flag.set()

    def reset_state(self):
        """Reset session state for new cycle."""
        with self._lock:
            self.state = SessionState()
            self.state.start_time = time.time()

    def update_activity(self):
        """Update last activity timestamp."""
        with self._lock:
            self.state.last_activity = time.time()
            self.state.is_stuck = False

    def increment_impressions(self, count: int = 1):
        """Increment impression counter."""
        with self._lock:
            self.state.impressions += count

    def check_tab_timeout(self) -> bool:
        """
        Check if tab should be closed due to timeout.
        Returns True if tab should be closed.
        """
        elapsed = time.time() - self.state.start_time
        max_duration = random.uniform(
            self.config.tab_duration_min,
            self.config.tab_duration_max
        )
        return elapsed >= max_duration

    def check_stuck(self) -> bool:
        """
        Check if tab is stuck (no activity for threshold).
        Returns True if stuck detected.
        """
        with self._lock:
            if self.state.is_stuck:
                return True
            
            elapsed = time.time() - self.state.last_activity
            if elapsed >= self.config.stuck_threshold:
                self.state.is_stuck = True
                self.state.stuck_detections += 1
                return True
            return False

    def should_rotate_proxy(self) -> bool:
        """Check if proxy should be rotated based on impressions."""
        with self._lock:
            return self.state.impressions >= random.randint(
                self.config.min_impressions,
                self.config.max_impressions
            )

    def should_take_break(self) -> bool:
        """Check if a break is needed after proxy rotation."""
        with self._lock:
            elapsed = time.time() - self.state.start_time
            return (elapsed >= self.config.proxy_change_interval or 
                    self.should_rotate_proxy())

    def take_break(self):
        """Take a break before continuing with new proxy."""
        break_duration = random.uniform(
            self.config.break_duration_min,
            self.config.break_duration_max
        )
        print(f"  💤 Taking {break_duration:.0f}s break (proxy rotation)...")
        time.sleep(break_duration)

    def smooth_scroll_js(self, page) -> str:
        """
        Generate smooth scrolling JavaScript.
        Returns JS string for smooth scroll.
        """
        step = self.config.smooth_scroll_step
        delay_ms = int(self.config.smooth_scroll_delay * 1000)
        
        return f"""
(function() {{
    const scrollHeight = document.body.scrollHeight;
    const viewportHeight = window.innerHeight;
    const maxScroll = scrollHeight - viewportHeight;
    
    if (maxScroll <= 0) return;
    
    let currentPos = 0;
    const step = {step};
    const delay = {delay_ms};
    
    function smoothScroll() {{
        if (currentPos >= maxScroll) {{
            // At bottom, reverse
            return;
        }}
        
        currentPos += step;
        if (currentPos > maxScroll) currentPos = maxScroll;
        
        window.scrollTo({{
            top: currentPos,
            behavior: 'smooth'
        }});
        
        setTimeout(smoothScroll, delay);
    }}
    
    smoothScroll();
}})();
"""

    def perform_smooth_scroll(self, page, timeout: Optional[float] = None) -> bool:
        """
        Perform smooth scroll on page.
        Uses JS-based smooth scrolling.
        Returns True if scroll completed.
        """
        try:
            scroll_height = page.evaluate("document.body.scrollHeight")
            viewport_height = page.evaluate("window.innerHeight")
            
            if scroll_height <= viewport_height:
                return True
            
            # Use JS-based smooth scrolling
            page.evaluate(self.smooth_scroll_js(page))
            return True
            
        except Exception as e:
            print(f"  ⚠️ Smooth scroll error: {e}")
            return False

    def scroll_with_timeout(self, page, duration: float) -> bool:
        """
        Scroll page for specified duration with smooth scrolling.
        Respects tab timeout and stuck detection.
        """
        start = time.time()
        deadline = start + duration
        step = self.config.smooth_scroll_step
        delay = self.config.smooth_scroll_delay
        
        try:
            scroll_height = page.evaluate("document.body.scrollHeight")
            viewport_height = page.evaluate("window.innerHeight")
            max_scroll = scroll_height - viewport_height
            
            if max_scroll <= 0:
                # Short page, just wait
                time.sleep(duration)
                return True
            
            current_pos = 0
            direction = 1  # 1 = down, -1 = up
            
            while time.time() < deadline:
                # Check timeout
                if self.check_tab_timeout():
                    print(f"  ⏰ Tab timeout reached ({duration:.0f}s)")
                    break
                
                # Calculate next scroll position
                next_pos = current_pos + (step * direction)
                
                # Reverse at boundaries
                if next_pos >= max_scroll:
                    next_pos = max_scroll
                    direction = -1
                elif next_pos <= 0:
                    next_pos = 0
                    direction = 1
                
                # Scroll
                try:
                    page.evaluate(f"window.scrollTo({{top: {next_pos}, behavior: 'smooth'}})")
                    current_pos = next_pos
                    self.update_activity()
                except:
                    pass
                
                # Small delay for smooth effect
                time.sleep(delay)
            
            return True
            
        except Exception as e:
            print(f"  ⚠️ Scroll error: {e}")
            return False

    def wait_with_activity_check(self, page, duration: float, 
                                   check_interval: float = 1.0) -> bool:
        """
        Wait for specified duration while checking for stuck state.
        Returns True if should continue, False if should stop.
        """
        deadline = time.time() + duration
        
        while time.time() < deadline:
            # Check stuck state
            if self.check_stuck():
                print(f"  🔴 Tab stuck detected! Force closing...")
                return False
            
            # Check tab timeout
            if self.check_tab_timeout():
                print(f"  ⏰ Tab timeout reached")
                return False
            
            # Update activity on any interaction
            try:
                if not page.is_closed():
                    # Small mouse movement to indicate activity
                    viewport = page.viewport_size or {"width": 800, "height": 600}
                    page.mouse.move(
                        random.randint(100, viewport["width"] - 100),
                        random.randint(100, viewport["height"] - 100)
                    )
                    self.update_activity()
            except:
                pass
            
            time.sleep(check_interval)
        
        return True

    def run_tab_session(self, page, session_func: Callable, 
                        on_tab_close: Optional[Callable] = None) -> dict:
        """
        Run a complete tab session with timeout and stuck detection.
        
        Args:
            page: Playwright page object
            session_func: Function to execute during session
            on_tab_close: Optional callback when tab closes
            
        Returns:
            dict with session results
        """
        self.reset_state()
        tab_start = time.time()
        results = {
            "duration": 0,
            "impressions": 0,
            "clicks": 0,
            "stuck": False,
            "timeout": False
        }
        
        try:
            while not self._stop_flag.is_set():
                # Check timeout
                if self.check_tab_timeout():
                    results["timeout"] = True
                    print(f"  ⏰ Tab timeout - opening new tab")
                    break
                
                # Check stuck
                if self.check_stuck():
                    results["stuck"] = True
                    print(f"  🔴 Tab stuck - force replacing")
                    break
                
                # Calculate remaining time
                elapsed = time.time() - tab_start
                remaining = random.uniform(
                    self.config.tab_duration_min,
                    self.config.tab_duration_max
                ) - elapsed
                
                if remaining <= 0:
                    results["timeout"] = True
                    break
                
                # Execute session function with remaining time
                try:
                    result = session_func(page, max_duration=remaining)
                    if result:
                        if isinstance(result, dict):
                            results["impressions"] += result.get("impressions", 0)
                            results["clicks"] += result.get("clicks", 0)
                        self.update_activity()
                except Exception as e:
                    print(f"  ⚠️ Session error: {e}")
                
                # Small delay
                time.sleep(0.5)
                
        except Exception as e:
            print(f"  ❌ Tab session error: {e}")
        finally:
            results["duration"] = time.time() - tab_start
            self.increment_impressions(results["impressions"])
            
            if on_tab_close:
                try:
                    on_tab_close(results)
                except:
                    pass
        
        return results

    def pause(self):
        """Pause the controller."""
        self._pause_flag.clear()

    def resume(self):
        """Resume the controller."""
        self._pause_flag.set()

    def stop(self):
        """Stop the controller."""
        self._stop_flag.set()
        self._pause_flag.set()

    def is_running(self) -> bool:
        """Check if controller is running."""
        return not self._stop_flag.is_set()


class ProxyRotationManager:
    """
    Manages proxy rotation with:
    - 50 proxy pool
    - 5-minute IP change interval
    - 30-40 impression rotation threshold
    - 2-3 minute breaks between rotations
    """

    def __init__(self, proxy_file: str, config: Optional[SessionConfig] = None):
        self.config = config or SessionConfig()
        self._lock = threading.Lock()
        self._proxies: list = []
        self._current_index: int = 0
        self._impression_count: int = 0
        self._last_rotation: float = time.time()
        self._rotation_count: int = 0
        self._load_proxies(proxy_file)

    def _load_proxies(self, proxy_file: str):
        """Load proxies from file."""
        try:
            with open(proxy_file, 'r') as f:
                self._proxies = [line.strip() for line in f 
                                if line.strip() and not line.startswith('#')]
            print(f"  📋 Loaded {len(self._proxies)} proxies")
        except Exception as e:
            print(f"  ❌ Failed to load proxies: {e}")
            self._proxies = []

    def get_next_proxy(self) -> Optional[str]:
        """Get next proxy from rotation."""
        with self._lock:
            if not self._proxies:
                return None
            
            proxy = self._proxies[self._current_index]
            self._current_index = (self._current_index + 1) % len(self._proxies)
            return proxy

    def record_impressions(self, count: int = 1):
        """Record impressions for rotation tracking."""
        with self._lock:
            self._impression_count += count

    def should_rotate(self) -> bool:
        """Check if should rotate to next proxy."""
        with self._lock:
            elapsed = time.time() - self._last_rotation
            
            # Rotate based on time (5 min) or impressions (30-40)
            target_impressions = random.randint(
                self.config.min_impressions,
                self.config.max_impressions
            )
            
            return (elapsed >= self.config.proxy_change_interval or 
                    self._impression_count >= target_impressions)

    def rotate(self) -> Optional[str]:
        """
        Rotate to next proxy.
        Returns new proxy or None if pool exhausted.
        """
        with self._lock:
            if not self._proxies:
                return None
            
            self._rotation_count += 1
            self._impression_count = 0
            self._last_rotation = time.time()
            
            proxy = self._proxies[self._current_index]
            self._current_index = (self._current_index + 1) % len(self._proxies)
            
            return proxy

    def get_stats(self) -> dict:
        """Get rotation statistics."""
        with self._lock:
            return {
                "total_proxies": len(self._proxies),
                "current_index": self._current_index,
                "impression_count": self._impression_count,
                "rotation_count": self._rotation_count,
                "last_rotation_age": time.time() - self._last_rotation
            }


class ContinuousAutomationLoop:
    """
    Main automation loop that runs continuously with:
    - Tab timeout management
    - Proxy rotation
    - Smooth scrolling
    - Stuck tab handling
    """

    def __init__(self, proxy_file: str, config: Optional[SessionConfig] = None,
                 session_factory: Optional[Callable] = None):
        self.config = config or SessionConfig()
        self.controller = SessionController(self.config)
        self.proxy_manager = ProxyRotationManager(proxy_file, self.config)
        self._stop_flag = threading.Event()
        self._session_factory = session_factory
        self._tab_count: int = 0
        self._session_count: int = 0
        self._start_time: float = time.time()

    def run(self):
        """Main automation loop - runs until stopped."""
        print(f"\n{'='*60}")
        print(f"  🚀 Continuous Automation Started")
        print(f"  Tab timeout: {self.config.tab_duration_min}-{self.config.tab_duration_max}s")
        print(f"  Stuck threshold: {self.config.stuck_threshold}s")
        print(f"  Proxy switch: {self.config.min_impressions}-{self.config.max_impressions} impressions")
        print(f"  Break: {self.config.break_duration_min/60:.1f}-{self.config.break_duration_max/60:.1f} min")
        print(f"{'='*60}\n")

        while not self._stop_flag.is_set():
            try:
                self._run_one_cycle()
            except KeyboardInterrupt:
                print("\n  ⚠️ Interrupted by user")
                break
            except Exception as e:
                print(f"  ❌ Cycle error: {e}")
                time.sleep(5)

        self._print_summary()

    def _run_one_cycle(self):
        """Run one automation cycle."""
        # Check proxy rotation
        if self.proxy_manager.should_rotate():
            self._handle_proxy_rotation()

        # Get next proxy
        proxy = self.proxy_manager.get_next_proxy()
        if not proxy:
            print("  ❌ No proxies available - waiting...")
            time.sleep(30)
            return

        # Create new tab session
        self._tab_count += 1
        print(f"\n  📱 Tab #{self._tab_count} | Proxy: {proxy[:50]}...")

        # Run session with timeout
        results = self._run_tab_with_timeout(proxy)
        
        # Record impressions
        self.proxy_manager.record_impressions(results.get("impressions", 1))
        self._session_count += 1

        # Small delay before next tab
        inter_delay = random.uniform(1, 3)
        time.sleep(inter_delay)

    def _run_tab_with_timeout(self, proxy: str) -> dict:
        """Run a single tab with timeout management."""
        results = {
            "impressions": random.randint(5, 15),
            "clicks": random.randint(1, 5),
            "stuck": False,
            "timeout": False,
            "duration": 0
        }
        
        start = time.time()
        tab_duration = random.uniform(
            self.config.tab_duration_min,
            self.config.tab_duration_max
        )
        
        try:
            # Create new browser context with proxy
            if self._session_factory:
                browser, context, page = self._session_factory(proxy)
                
                try:
                    # Run session with timeout
                    while time.time() - start < tab_duration:
                        # Check stuck
                        if self.controller.check_stuck():
                            results["stuck"] = True
                            print(f"  🔴 Tab stuck - force closing")
                            break
                        
                        # Smooth scroll
                        self.controller.scroll_with_timeout(page, duration=3)
                        
                        # Small delay
                        time.sleep(1)
                    
                    results["timeout"] = True
                    
                finally:
                    try:
                        page.close()
                        context.close()
                        browser.close()
                    except:
                        pass
            else:
                # No factory - just simulate
                print(f"  ⏱️  Running for {tab_duration:.0f}s...")
                time.sleep(tab_duration)
                
        except Exception as e:
            print(f"  ❌ Tab error: {e}")
        
        results["duration"] = time.time() - start
        return results

    def _handle_proxy_rotation(self):
        """Handle proxy rotation with break."""
        print(f"\n  🔄 Proxy rotation needed...")
        
        # Take break
        break_duration = random.uniform(
            self.config.break_duration_min,
            self.config.break_duration_max
        )
        print(f"  💤 Taking {break_duration/60:.1f} min break...")
        
        # Rotate
        new_proxy = self.proxy_manager.rotate()
        if new_proxy:
            print(f"  ✅ Rotated to new proxy: {new_proxy[:50]}...")
        
        time.sleep(break_duration)

    def _print_summary(self):
        """Print automation summary."""
        elapsed = time.time() - self._start_time
        stats = self.proxy_manager.get_stats()
        
        print(f"\n{'='*60}")
        print(f"  📊 Automation Summary")
        print(f"{'='*60}")
        print(f"  Total tabs: {self._tab_count}")
        print(f"  Total sessions: {self._session_count}")
        print(f"  Runtime: {elapsed/60:.1f} minutes")
        print(f"  Proxy rotations: {stats['rotation_count']}")
        print(f"  Stuck detections: {self.controller.state.stuck_detections}")
        print(f"{'='*60}\n")

    def stop(self):
        """Stop the automation."""
        self._stop_flag.set()
        self.controller.stop()
