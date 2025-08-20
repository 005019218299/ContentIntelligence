import asyncio
import psutil
import time
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
import logging
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class ResourceType(str, Enum):
    CPU = "cpu"
    GPU = "gpu"
    MEMORY = "memory"
    STORAGE = "storage"

@dataclass
class ResourceMetrics:
    cpu_usage: float
    memory_usage: float
    gpu_usage: float
    storage_usage: float
    network_io: float
    timestamp: datetime

class NeuralResourceOrchestrator:
    def __init__(self, config: Dict):
        self.config = config
        self.resource_history = []
        self.optimization_model = None
        self.resource_predictions = {}
        self.allocation_strategy = config.get('strategy', 'adaptive')
        
    async def monitor_resources(self) -> ResourceMetrics:
        """Monitor current resource usage"""
        try:
            cpu_usage = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            
            # Simulate GPU usage (would use nvidia-ml-py in real implementation)
            gpu_usage = np.random.uniform(20, 80) if self._gpu_available() else 0
            
            disk = psutil.disk_usage('/')
            network = psutil.net_io_counters()
            
            metrics = ResourceMetrics(
                cpu_usage=cpu_usage,
                memory_usage=memory.percent,
                gpu_usage=gpu_usage,
                storage_usage=(disk.used / disk.total) * 100,
                network_io=network.bytes_sent + network.bytes_recv,
                timestamp=datetime.now(timezone.utc)
            )
            
            # Store for trend analysis
            self.resource_history.append(metrics)
            if len(self.resource_history) > 1000:  # Keep last 1000 measurements
                self.resource_history.pop(0)
            
            return metrics
            
        except Exception as e:
            logger.error(f"Resource monitoring failed: {e}")
            return ResourceMetrics(0, 0, 0, 0, 0, datetime.now(timezone.utc))
    
    async def predict_resource_needs(self, time_horizon_minutes: int = 30) -> Dict[str, Any]:
        """Predict future resource needs using neural model"""
        try:
            if len(self.resource_history) < 10:
                return {
                    'prediction_available': False,
                    'reason': 'Insufficient historical data'
                }
            
            # Extract features from recent history
            recent_metrics = self.resource_history[-10:]
            features = await self._extract_prediction_features(recent_metrics)
            
            # Predict future resource usage
            predictions = await self._neural_predict(features, time_horizon_minutes)
            
            # Generate recommendations
            recommendations = await self._generate_resource_recommendations(predictions)
            
            return {
                'prediction_available': True,
                'time_horizon_minutes': time_horizon_minutes,
                'predicted_usage': predictions,
                'recommendations': recommendations,
                'confidence': self._calculate_prediction_confidence()
            }
            
        except Exception as e:
            logger.error(f"Resource prediction failed: {e}")
            return {'prediction_available': False, 'error': str(e)}
    
    async def _extract_prediction_features(self, metrics: List[ResourceMetrics]) -> np.ndarray:
        """Extract features for prediction model"""
        features = []
        
        for metric in metrics:
            feature_vector = [
                metric.cpu_usage / 100,
                metric.memory_usage / 100,
                metric.gpu_usage / 100,
                metric.storage_usage / 100,
                time.mktime(metric.timestamp.timetuple()) % 86400 / 86400,  # Time of day
                metric.timestamp.weekday() / 7  # Day of week
            ]
            features.append(feature_vector)
        
        return np.array(features)
    
    async def _neural_predict(self, features: np.ndarray, horizon: int) -> Dict[str, float]:
        """Neural network prediction of resource usage"""
        # Simulate neural network prediction
        await asyncio.sleep(0.05)  # Prediction computation time
        
        # Simple trend-based prediction
        current_usage = features[-1]  # Latest measurements
        trend = np.mean(np.diff(features[-5:], axis=0), axis=0) if len(features) >= 5 else np.zeros(6)
        
        # Project trend forward
        future_usage = current_usage + trend * (horizon / 30)  # Scale by time horizon
        
        return {
            'cpu_usage': max(0, min(100, future_usage[0] * 100)),
            'memory_usage': max(0, min(100, future_usage[1] * 100)),
            'gpu_usage': max(0, min(100, future_usage[2] * 100)),
            'storage_usage': max(0, min(100, future_usage[3] * 100))
        }
    
    async def _generate_resource_recommendations(self, predictions: Dict[str, float]) -> List[Dict[str, Any]]:
        """Generate resource optimization recommendations"""
        recommendations = []
        
        # CPU recommendations
        if predictions['cpu_usage'] > 85:
            recommendations.append({
                'resource': 'CPU',
                'action': 'scale_up',
                'urgency': 'high',
                'description': 'CPU usage predicted to exceed 85%',
                'suggested_action': 'Add 2 more CPU cores or scale horizontally'
            })
        elif predictions['cpu_usage'] < 30:
            recommendations.append({
                'resource': 'CPU',
                'action': 'scale_down',
                'urgency': 'low',
                'description': 'CPU usage predicted to be below 30%',
                'suggested_action': 'Consider reducing CPU allocation'
            })
        
        # Memory recommendations
        if predictions['memory_usage'] > 90:
            recommendations.append({
                'resource': 'Memory',
                'action': 'scale_up',
                'urgency': 'critical',
                'description': 'Memory usage predicted to exceed 90%',
                'suggested_action': 'Increase memory allocation immediately'
            })
        
        # GPU recommendations
        if predictions['gpu_usage'] > 80:
            recommendations.append({
                'resource': 'GPU',
                'action': 'optimize',
                'urgency': 'medium',
                'description': 'GPU usage predicted to be high',
                'suggested_action': 'Enable model quantization or batch optimization'
            })
        
        return recommendations
    
    def _calculate_prediction_confidence(self) -> float:
        """Calculate confidence in predictions based on data quality"""
        if len(self.resource_history) < 50:
            return 0.6  # Low confidence with limited data
        
        # Calculate variance in recent measurements
        recent_cpu = [m.cpu_usage for m in self.resource_history[-20:]]
        cpu_variance = np.var(recent_cpu)
        
        # Lower variance = higher confidence
        confidence = max(0.5, 1.0 - (cpu_variance / 1000))
        return min(0.95, confidence)
    
    def _gpu_available(self) -> bool:
        """Check if GPU is available"""
        try:
            import torch
            return torch.cuda.is_available()
        except ImportError:
            return False

class WorkloadAwareScheduler:
    def __init__(self, config: Dict):
        self.config = config
        self.job_queue = asyncio.PriorityQueue()
        self.active_jobs = {}
        self.scheduling_history = []
        
    async def schedule_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """Schedule job based on workload and resource availability"""
        try:
            job_priority = await self._calculate_job_priority(job)
            job_id = job.get('id', f"job_{int(time.time())}")
            
            # Add to queue with priority
            await self.job_queue.put((job_priority, job_id, job))
            
            # Try to execute if resources available
            execution_result = await self._try_execute_jobs()
            
            return {
                'job_scheduled': True,
                'job_id': job_id,
                'priority': job_priority,
                'queue_position': self.job_queue.qsize(),
                'execution_result': execution_result
            }
            
        except Exception as e:
            logger.error(f"Job scheduling failed: {e}")
            return {'job_scheduled': False, 'error': str(e)}
    
    async def _calculate_job_priority(self, job: Dict[str, Any]) -> int:
        """Calculate job priority based on multiple factors"""
        priority = 100  # Base priority
        
        # Job type priority
        job_type = job.get('type', 'inference')
        type_priorities = {
            'training': 50,      # Lower priority (higher number)
            'inference': 10,     # Higher priority (lower number)
            'batch_processing': 30,
            'model_optimization': 40
        }
        priority += type_priorities.get(job_type, 50)
        
        # Urgency factor
        urgency = job.get('urgency', 'normal')
        urgency_modifiers = {
            'critical': -30,
            'high': -15,
            'normal': 0,
            'low': 15
        }
        priority += urgency_modifiers.get(urgency, 0)
        
        # Resource requirements (prefer lighter jobs when resources are constrained)
        resource_weight = job.get('resource_weight', 1.0)
        if resource_weight > 2.0:
            priority += 20  # Deprioritize heavy jobs
        
        return max(1, priority)  # Ensure positive priority
    
    async def _try_execute_jobs(self) -> Dict[str, Any]:
        """Try to execute queued jobs based on available resources"""
        executed_jobs = []
        
        # Check resource availability
        current_metrics = await self._get_current_resource_usage()
        
        while not self.job_queue.empty() and self._can_execute_job(current_metrics):
            try:
                priority, job_id, job = await asyncio.wait_for(self.job_queue.get(), timeout=0.1)
                
                # Execute job
                execution_start = time.time()
                result = await self._execute_job(job)
                execution_time = time.time() - execution_start
                
                executed_jobs.append({
                    'job_id': job_id,
                    'execution_time': execution_time,
                    'result': result
                })
                
                # Update resource usage
                current_metrics = await self._update_resource_usage(job, current_metrics)
                
            except asyncio.TimeoutError:
                break
            except Exception as e:
                logger.error(f"Job execution failed: {e}")
        
        return {
            'jobs_executed': len(executed_jobs),
            'executed_jobs': executed_jobs,
            'remaining_queue_size': self.job_queue.qsize()
        }
    
    async def _get_current_resource_usage(self) -> Dict[str, float]:
        """Get current resource usage"""
        return {
            'cpu': psutil.cpu_percent(),
            'memory': psutil.virtual_memory().percent,
            'gpu': np.random.uniform(20, 80)  # Simulated GPU usage
        }
    
    def _can_execute_job(self, current_metrics: Dict[str, float]) -> bool:
        """Check if system can handle another job"""
        thresholds = self.config.get('resource_thresholds', {
            'cpu': 80,
            'memory': 85,
            'gpu': 90
        })
        
        return all(
            current_metrics.get(resource, 0) < threshold
            for resource, threshold in thresholds.items()
        )
    
    async def _execute_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single job"""
        job_type = job.get('type', 'inference')
        
        # Simulate job execution
        if job_type == 'training':
            await asyncio.sleep(2.0)  # Training takes longer
        elif job_type == 'inference':
            await asyncio.sleep(0.1)  # Inference is fast
        else:
            await asyncio.sleep(0.5)  # Default execution time
        
        return {
            'status': 'completed',
            'job_type': job_type,
            'output': f"Result for {job.get('id', 'unknown')}"
        }
    
    async def _update_resource_usage(self, job: Dict[str, Any], current_metrics: Dict[str, float]) -> Dict[str, float]:
        """Update resource usage after job execution"""
        # Simulate resource consumption
        resource_impact = job.get('resource_weight', 1.0)
        
        return {
            'cpu': min(100, current_metrics['cpu'] + resource_impact * 10),
            'memory': min(100, current_metrics['memory'] + resource_impact * 5),
            'gpu': min(100, current_metrics['gpu'] + resource_impact * 15)
        }

class CarbonAwareScheduler:
    def __init__(self, config: Dict):
        self.config = config
        self.carbon_intensity_data = {}
        self.green_energy_schedule = {}
        
    async def get_carbon_optimal_schedule(self, jobs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Schedule jobs to minimize carbon footprint"""
        try:
            # Get current carbon intensity
            current_intensity = await self._get_carbon_intensity()
            
            # Find optimal scheduling windows
            optimal_windows = await self._find_green_windows(jobs)
            
            # Schedule jobs in green windows
            scheduled_jobs = await self._schedule_in_green_windows(jobs, optimal_windows)
            
            # Calculate carbon savings
            carbon_savings = await self._calculate_carbon_savings(scheduled_jobs)
            
            return {
                'carbon_scheduling_completed': True,
                'current_carbon_intensity': current_intensity,
                'scheduled_jobs': scheduled_jobs,
                'carbon_savings': carbon_savings,
                'green_windows': optimal_windows
            }
            
        except Exception as e:
            logger.error(f"Carbon-aware scheduling failed: {e}")
            return {'carbon_scheduling_completed': False, 'error': str(e)}
    
    async def _get_carbon_intensity(self) -> Dict[str, Any]:
        """Get current carbon intensity data"""
        # Simulate carbon intensity API call
        await asyncio.sleep(0.1)
        
        # Mock carbon intensity data (gCO2/kWh)
        current_hour = datetime.now().hour
        
        # Lower intensity during day (solar), higher at night
        base_intensity = 400
        if 8 <= current_hour <= 18:  # Daytime
            intensity = base_intensity - 100 + np.random.uniform(-50, 50)
        else:  # Nighttime
            intensity = base_intensity + 50 + np.random.uniform(-30, 30)
        
        return {
            'intensity_gco2_kwh': max(200, intensity),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'source': 'renewable' if intensity < 350 else 'mixed'
        }
    
    async def _find_green_windows(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Find time windows with lowest carbon intensity"""
        green_windows = []
        
        # Simulate 24-hour carbon intensity forecast
        for hour in range(24):
            # Lower intensity during typical solar hours
            if 9 <= hour <= 17:
                intensity = 250 + np.random.uniform(-50, 50)
                renewable_percentage = 70 + np.random.uniform(-10, 10)
            else:
                intensity = 450 + np.random.uniform(-50, 50)
                renewable_percentage = 30 + np.random.uniform(-10, 10)
            
            if intensity < 350:  # Green threshold
                green_windows.append({
                    'start_hour': hour,
                    'end_hour': hour + 1,
                    'carbon_intensity': intensity,
                    'renewable_percentage': renewable_percentage,
                    'capacity': 10  # Max jobs per hour
                })
        
        return sorted(green_windows, key=lambda x: x['carbon_intensity'])
    
    async def _schedule_in_green_windows(self, jobs: List[Dict[str, Any]], windows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Schedule jobs in green energy windows"""
        scheduled = []
        
        for job in jobs:
            job_duration = job.get('estimated_duration_hours', 1)
            job_priority = job.get('carbon_priority', 'normal')
            
            # Find suitable window
            for window in windows:
                if window['capacity'] >= job_duration:
                    scheduled.append({
                        'job_id': job.get('id'),
                        'scheduled_start': window['start_hour'],
                        'scheduled_end': window['end_hour'],
                        'carbon_intensity': window['carbon_intensity'],
                        'renewable_percentage': window['renewable_percentage'],
                        'carbon_priority': job_priority
                    })
                    
                    window['capacity'] -= job_duration
                    break
        
        return scheduled
    
    async def _calculate_carbon_savings(self, scheduled_jobs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate carbon footprint savings from green scheduling"""
        total_jobs = len(scheduled_jobs)
        
        if total_jobs == 0:
            return {'total_savings_kg_co2': 0, 'percentage_reduction': 0}
        
        # Calculate savings vs. immediate execution
        immediate_intensity = 450  # Assume current high intensity
        green_avg_intensity = np.mean([job['carbon_intensity'] for job in scheduled_jobs])
        
        # Assume 1 kWh per job (simplified)
        immediate_emissions = total_jobs * immediate_intensity / 1000  # kg CO2
        green_emissions = total_jobs * green_avg_intensity / 1000  # kg CO2
        
        savings = immediate_emissions - green_emissions
        percentage_reduction = (savings / immediate_emissions) * 100 if immediate_emissions > 0 else 0
        
        return {
            'total_savings_kg_co2': max(0, savings),
            'percentage_reduction': max(0, percentage_reduction),
            'immediate_emissions_kg_co2': immediate_emissions,
            'green_emissions_kg_co2': green_emissions,
            'jobs_scheduled': total_jobs
        }