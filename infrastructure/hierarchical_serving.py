import asyncio
import time
import psutil
import torch
from typing import Dict, List, Any, Optional, Tuple
import logging
from datetime import datetime, timezone
from dataclasses import dataclass
from enum import Enum
import numpy as np

logger = logging.getLogger(__name__)

class ServingTier(str, Enum):
    FAST = "fast"
    BALANCED = "balanced"
    ACCURATE = "accurate"

@dataclass
class InferenceRequest:
    request_id: str
    content_data: Dict[str, Any]
    priority: int
    max_latency_ms: int
    min_accuracy: float
    timestamp: datetime

class HierarchicalModelServer:
    def __init__(self, config: Dict):
        self.config = config
        self.model_tiers = {
            ServingTier.FAST: {'latency': 50, 'accuracy': 0.85, 'gpu_usage': 0.2},
            ServingTier.BALANCED: {'latency': 150, 'accuracy': 0.92, 'gpu_usage': 0.5},
            ServingTier.ACCURATE: {'latency': 500, 'accuracy': 0.97, 'gpu_usage': 1.0}
        }
        self.current_load = 0.0
        self.request_queue = asyncio.Queue()
        self.processing_stats = {
            'total_requests': 0,
            'tier_usage': {tier: 0 for tier in ServingTier},
            'avg_latency': 0.0
        }
        
    async def route_request(self, request: InferenceRequest) -> Dict[str, Any]:
        """Route request to appropriate model tier"""
        try:
            # Determine optimal tier
            selected_tier = await self._select_tier(request)
            
            # Process request
            start_time = time.time()
            result = await self._process_with_tier(request, selected_tier)
            processing_time = (time.time() - start_time) * 1000
            
            # Update statistics
            await self._update_stats(selected_tier, processing_time)
            
            return {
                'inference_completed': True,
                'request_id': request.request_id,
                'selected_tier': selected_tier.value,
                'processing_time_ms': processing_time,
                'result': result,
                'tier_metrics': self.model_tiers[selected_tier]
            }
            
        except Exception as e:
            logger.error(f"Request routing failed: {e}")
            return {'inference_completed': False, 'error': str(e)}
    
    async def _select_tier(self, request: InferenceRequest) -> ServingTier:
        """Select appropriate tier based on request requirements and system load"""
        # Check system load
        cpu_usage = psutil.cpu_percent()
        memory_usage = psutil.virtual_memory().percent
        
        # Adjust for system load
        if cpu_usage > 80 or memory_usage > 85:
            # High load - prefer fast tier
            if request.max_latency_ms <= 100:
                return ServingTier.FAST
            elif request.min_accuracy <= 0.9:
                return ServingTier.FAST
        
        # Normal load - select based on requirements
        for tier in [ServingTier.FAST, ServingTier.BALANCED, ServingTier.ACCURATE]:
            tier_info = self.model_tiers[tier]
            if (tier_info['latency'] <= request.max_latency_ms and 
                tier_info['accuracy'] >= request.min_accuracy):
                return tier
        
        # Fallback to fast tier
        return ServingTier.FAST
    
    async def _process_with_tier(self, request: InferenceRequest, tier: ServingTier) -> Dict[str, Any]:
        """Process request with selected tier"""
        # Simulate processing time based on tier
        tier_info = self.model_tiers[tier]
        processing_delay = tier_info['latency'] / 1000  # Convert to seconds
        
        await asyncio.sleep(processing_delay)
        
        # Simulate inference result
        content_score = np.random.uniform(0.7, 0.95)
        confidence = tier_info['accuracy'] + np.random.uniform(-0.05, 0.05)
        
        return {
            'content_score': content_score,
            'confidence': max(0, min(1, confidence)),
            'tier_used': tier.value,
            'model_version': f"{tier.value}_v1.0"
        }
    
    async def _update_stats(self, tier: ServingTier, processing_time: float):
        """Update processing statistics"""
        self.processing_stats['total_requests'] += 1
        self.processing_stats['tier_usage'][tier] += 1
        
        # Update average latency
        total_requests = self.processing_stats['total_requests']
        current_avg = self.processing_stats['avg_latency']
        self.processing_stats['avg_latency'] = (
            (current_avg * (total_requests - 1) + processing_time) / total_requests
        )

class ServerlessInferenceManager:
    def __init__(self, config: Dict):
        self.config = config
        self.function_pools = {}
        self.cold_start_cache = {}
        self.scaling_metrics = {
            'active_functions': 0,
            'cold_starts': 0,
            'warm_hits': 0
        }
        
    async def invoke_function(self, function_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Invoke serverless inference function"""
        try:
            # Check if function is warm
            is_warm = function_name in self.cold_start_cache
            
            if not is_warm:
                # Cold start
                await self._cold_start_function(function_name)
                self.scaling_metrics['cold_starts'] += 1
            else:
                self.scaling_metrics['warm_hits'] += 1
            
            # Execute function
            start_time = time.time()
            result = await self._execute_function(function_name, payload)
            execution_time = (time.time() - start_time) * 1000
            
            return {
                'function_execution_completed': True,
                'function_name': function_name,
                'execution_time_ms': execution_time,
                'cold_start': not is_warm,
                'result': result,
                'scaling_metrics': self.scaling_metrics
            }
            
        except Exception as e:
            logger.error(f"Serverless function invocation failed: {e}")
            return {'function_execution_completed': False, 'error': str(e)}
    
    async def _cold_start_function(self, function_name: str):
        """Simulate cold start of serverless function"""
        # Simulate cold start delay
        await asyncio.sleep(0.5)  # 500ms cold start
        
        # Cache function as warm
        self.cold_start_cache[function_name] = {
            'started_at': datetime.now(timezone.utc),
            'invocation_count': 0
        }
        
        self.scaling_metrics['active_functions'] += 1
    
    async def _execute_function(self, function_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Execute serverless function"""
        # Update invocation count
        if function_name in self.cold_start_cache:
            self.cold_start_cache[function_name]['invocation_count'] += 1
        
        # Simulate function execution
        await asyncio.sleep(0.1)  # 100ms execution time
        
        return {
            'prediction_score': np.random.uniform(0.7, 0.95),
            'processing_node': f"serverless_{function_name}",
            'function_version': "v1.0"
        }
    
    async def auto_scale_functions(self) -> Dict[str, Any]:
        """Auto-scale serverless functions based on load"""
        try:
            current_load = len(self.cold_start_cache)
            target_capacity = max(2, int(current_load * 1.2))  # 20% buffer
            
            scaling_actions = []
            
            # Scale up if needed
            if current_load < target_capacity:
                for i in range(target_capacity - current_load):
                    new_function = f"inference_function_{len(self.cold_start_cache) + i}"
                    await self._cold_start_function(new_function)
                    scaling_actions.append(f"Started {new_function}")
            
            # Scale down idle functions
            idle_functions = []
            for func_name, func_info in self.cold_start_cache.items():
                if func_info['invocation_count'] == 0:
                    idle_functions.append(func_name)
            
            for func_name in idle_functions[:len(idle_functions)//2]:  # Scale down 50% of idle
                del self.cold_start_cache[func_name]
                self.scaling_metrics['active_functions'] -= 1
                scaling_actions.append(f"Stopped {func_name}")
            
            return {
                'auto_scaling_completed': True,
                'current_capacity': len(self.cold_start_cache),
                'target_capacity': target_capacity,
                'scaling_actions': scaling_actions,
                'metrics': self.scaling_metrics
            }
            
        except Exception as e:
            logger.error(f"Auto-scaling failed: {e}")
            return {'auto_scaling_completed': False, 'error': str(e)}

class GPUCPUAutoSwitcher:
    def __init__(self, config: Dict):
        self.config = config
        self.gpu_available = torch.cuda.is_available()
        self.cpu_threshold = config.get('cpu_threshold', 0.3)  # Switch to CPU if load < 30%
        self.gpu_threshold = config.get('gpu_threshold', 0.7)  # Switch to GPU if load > 70%
        self.current_device = 'cpu'
        
    async def select_compute_device(self, workload_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Select optimal compute device based on workload"""
        try:
            current_load = workload_metrics.get('request_rate', 0.5)
            batch_size = workload_metrics.get('batch_size', 1)
            model_complexity = workload_metrics.get('model_complexity', 'medium')
            
            # Decision logic
            recommended_device = await self._evaluate_device_selection(
                current_load, batch_size, model_complexity
            )
            
            # Switch device if needed
            device_switched = False
            if recommended_device != self.current_device:
                await self._switch_device(recommended_device)
                device_switched = True
            
            return {
                'device_selection_completed': True,
                'current_device': self.current_device,
                'recommended_device': recommended_device,
                'device_switched': device_switched,
                'selection_reason': self._get_selection_reason(current_load, batch_size, model_complexity),
                'performance_estimate': self._estimate_performance(self.current_device, workload_metrics)
            }
            
        except Exception as e:
            logger.error(f"Device selection failed: {e}")
            return {'device_selection_completed': False, 'error': str(e)}
    
    async def _evaluate_device_selection(self, load: float, batch_size: int, complexity: str) -> str:
        """Evaluate optimal device selection"""
        if not self.gpu_available:
            return 'cpu'
        
        # GPU preferred for high load, large batches, complex models
        gpu_score = 0
        
        if load > self.gpu_threshold:
            gpu_score += 3
        elif load > self.cpu_threshold:
            gpu_score += 1
        
        if batch_size > 8:
            gpu_score += 2
        elif batch_size > 4:
            gpu_score += 1
        
        if complexity == 'high':
            gpu_score += 2
        elif complexity == 'medium':
            gpu_score += 1
        
        return 'gpu' if gpu_score >= 4 else 'cpu'
    
    async def _switch_device(self, target_device: str):
        """Switch compute device"""
        # Simulate device switching
        await asyncio.sleep(0.1)  # Device switching overhead
        self.current_device = target_device
        logger.info(f"Switched to {target_device}")
    
    def _get_selection_reason(self, load: float, batch_size: int, complexity: str) -> str:
        """Get human-readable reason for device selection"""
        reasons = []
        
        if load > self.gpu_threshold:
            reasons.append("High request load")
        elif load < self.cpu_threshold:
            reasons.append("Low request load")
        
        if batch_size > 8:
            reasons.append("Large batch size")
        elif batch_size == 1:
            reasons.append("Single request processing")
        
        if complexity == 'high':
            reasons.append("Complex model computation")
        
        return "; ".join(reasons) or "Balanced workload"
    
    def _estimate_performance(self, device: str, workload: Dict[str, Any]) -> Dict[str, float]:
        """Estimate performance metrics for selected device"""
        if device == 'gpu':
            return {
                'estimated_latency_ms': 80,
                'throughput_rps': 50,
                'energy_efficiency': 0.7
            }
        else:
            return {
                'estimated_latency_ms': 200,
                'throughput_rps': 20,
                'energy_efficiency': 0.9
            }

class MicroBatchingEngine:
    def __init__(self, config: Dict):
        self.config = config
        self.batch_size = config.get('max_batch_size', 16)
        self.batch_timeout_ms = config.get('batch_timeout_ms', 50)
        self.pending_requests = []
        self.batch_stats = {
            'total_batches': 0,
            'avg_batch_size': 0,
            'throughput_improvement': 0
        }
        
    async def add_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Add request to micro-batch"""
        try:
            request['arrival_time'] = time.time()
            self.pending_requests.append(request)
            
            # Check if batch is ready
            if len(self.pending_requests) >= self.batch_size:
                return await self._process_batch()
            
            # Set timeout for partial batch
            asyncio.create_task(self._batch_timeout_handler())
            
            return {
                'request_queued': True,
                'queue_position': len(self.pending_requests),
                'estimated_wait_ms': self.batch_timeout_ms
            }
            
        except Exception as e:
            logger.error(f"Micro-batching failed: {e}")
            return {'request_queued': False, 'error': str(e)}
    
    async def _process_batch(self) -> Dict[str, Any]:
        """Process accumulated micro-batch"""
        if not self.pending_requests:
            return {'batch_processed': False, 'reason': 'No pending requests'}
        
        batch = self.pending_requests.copy()
        self.pending_requests.clear()
        
        # Simulate batch processing
        start_time = time.time()
        await asyncio.sleep(0.1)  # Batch processing time
        processing_time = (time.time() - start_time) * 1000
        
        # Update statistics
        self.batch_stats['total_batches'] += 1
        current_avg = self.batch_stats['avg_batch_size']
        total_batches = self.batch_stats['total_batches']
        self.batch_stats['avg_batch_size'] = (
            (current_avg * (total_batches - 1) + len(batch)) / total_batches
        )
        
        # Calculate throughput improvement
        single_request_time = 100  # Assume 100ms per single request
        batch_efficiency = (len(batch) * single_request_time) / processing_time
        self.batch_stats['throughput_improvement'] = batch_efficiency
        
        return {
            'batch_processed': True,
            'batch_size': len(batch),
            'processing_time_ms': processing_time,
            'throughput_improvement': batch_efficiency,
            'batch_stats': self.batch_stats
        }
    
    async def _batch_timeout_handler(self):
        """Handle batch timeout for partial batches"""
        await asyncio.sleep(self.batch_timeout_ms / 1000)
        
        if self.pending_requests:  # Still have pending requests after timeout
            await self._process_batch()