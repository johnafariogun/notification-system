"""Monitoring and metrics collection"""
from typing import Dict, Any, List
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict

@dataclass
class ServiceMetrics:
    """Service performance metrics"""
    total_sent: int = 0
    total_failed: int = 0
    total_retried: int = 0
    total_queued: int = 0
    avg_processing_time_ms: float = 0.0
    success_rate: float = 0.0
    circuit_breaker_trips: int = 0
    last_updated: str = ""

class MetricsCollector:
    """Collects and aggregates service metrics"""
    
    def __init__(self):
        self.metrics = ServiceMetrics()
        self.processing_times: List[float] = []
        self.hourly_counts: Dict[str, int] = {}
    
    def record_sent(self, processing_time_ms: float):
        """Record successful notification"""
        self.metrics.total_sent += 1
        self.processing_times.append(processing_time_ms)
        self._update_avg_processing_time()
        self._update_success_rate()
    
    def record_failed(self):
        """Record failed notification"""
        self.metrics.total_failed += 1
        self._update_success_rate()
    
    def record_retry(self):
        """Record retry attempt"""
        self.metrics.total_retried += 1
    
    def _update_avg_processing_time(self):
        """Calculate average processing time"""
        if self.processing_times:
            recent_times = self.processing_times[-100:]
            self.metrics.avg_processing_time_ms = sum(recent_times) / len(recent_times)
    
    def _update_success_rate(self):
        """Calculate success rate"""
        total = self.metrics.total_sent + self.metrics.total_failed
        if total > 0:
            self.metrics.success_rate = (self.metrics.total_sent / total) * 100
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics"""
        self.metrics.last_updated = datetime.utcnow().isoformat()
        return asdict(self.metrics)
