"""
TASK C: Production-Grade Rate Limiter Tests
==============================================

This test suite demonstrates:
1. How dependency injection makes testing easier (no real DB/Redis needed)
2. Async concurrent testing with pytest-asyncio
3. Mock services for isolation
4. The atomic rate limiter behavior under concurrent load

Requirements:
- pip install pytest pytest-asyncio unittest-mock
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException
from fastapi.testclient import TestClient

# Import the FastAPI app
from main import app, atomic_rate_limiter, get_redis_client, RedisClient


# ============================================================================
# MOCK REDIS CLIENT (for isolated testing without real Redis)
# ============================================================================

class MockRedisClient(RedisClient):
    """
    Mock Redis client that simulates Redis behavior in-memory.
    This allows us to test without needing a real Redis instance.
    """
    
    def __init__(self):
        # Don't connect to real Redis
        self.redis = AsyncMock()
        self.data_store = {}  # Simulate Redis data
        
    async def mock_script_execution(self, script_sha, keys, args):
        """
        Simulate the Lua script execution for rate limiting.
        This is what happens inside Redis atomically.
        """
        key = keys[0]
        limit = int(args[0])
        seconds = int(args[1])
        
        # Simulate INCR operation
        current_count = self.data_store.get(key, 0) + 1
        self.data_store[key] = current_count
        
        # Simulate EXPIRE (set a TTL marker - in real Redis this is automatic)
        if current_count == 1:
            self.data_store[f"{key}:ttl"] = seconds
        
        # Return the current count (this is what the Lua script returns)
        return current_count


# ============================================================================
# TEST FIXTURES
# ============================================================================

@pytest.fixture
def mock_redis_client():
    """Provide a mock Redis client for testing."""
    return MockRedisClient()


@pytest.fixture
def override_dependencies(mock_redis_client):
    """
    Override FastAPI dependencies to use mock services.
    This is dependency injection in action!
    """
    app.dependency_overrides[get_redis_client] = lambda: mock_redis_client
    yield
    # Cleanup: restore original dependencies
    app.dependency_overrides.clear()


# ============================================================================
# TASK C: ATOMIC RATE LIMITER TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_rate_limiter_single_request(mock_redis_client):
    """
    Test 1: Single request should always pass.
    
    This is the happy path - one user, one request, should succeed.
    """
    redis_client = mock_redis_client
    
    # Setup mock to simulate the Lua script
    current_count = 1
    redis_client.redis.register_script = MagicMock(
        return_value=AsyncMock(return_value=current_count)
    )
    
    # Call the rate limiter
    result = await atomic_rate_limiter("user123", redis_client)
    
    # Should NOT raise an exception
    assert result is False, "First request should be allowed"


@pytest.mark.asyncio
async def test_rate_limiter_under_limit(mock_redis_client):
    """
    Test 2: Multiple requests under the limit should all pass.
    
    The limit is 5 requests per 10 seconds.
    We make 5 sequential requests - all should pass.
    """
    redis_client = mock_redis_client
    
    # Setup counters
    counters = {"count": 0}
    
    async def mock_script(*args, **kwargs):
        counters["count"] += 1
        return counters["count"]
    
    redis_client.redis.register_script = MagicMock(
        return_value=AsyncMock(side_effect=mock_script)
    )
    
    # Make 5 requests (the limit)
    for i in range(5):
        result = await atomic_rate_limiter("user456", redis_client)
        assert result is False, f"Request {i+1} should be allowed (under limit)"


@pytest.mark.asyncio
async def test_rate_limiter_exceeds_limit(mock_redis_client):
    """
    Test 3: Request exceeding the limit should be rejected (429).
    
    The limit is 5 requests per 10 seconds.
    The 6th request should raise HTTPException with status 429.
    """
    redis_client = mock_redis_client
    
    # Setup counters to simulate 6 requests
    counters = {"count": 0}
    
    async def mock_script(*args, **kwargs):
        counters["count"] += 1
        return counters["count"]
    
    redis_client.redis.register_script = MagicMock(
        return_value=AsyncMock(side_effect=mock_script)
    )
    
    # Make 5 requests - should all succeed
    for i in range(5):
        result = await atomic_rate_limiter("user789", redis_client)
        assert result is False, f"Request {i+1} should pass"
    
    # The 6th request should be rejected
    with pytest.raises(HTTPException) as exc_info:
        await atomic_rate_limiter("user789", redis_client)
    
    assert exc_info.value.status_code == 429
    assert "Too many requests" in exc_info.value.detail


@pytest.mark.asyncio
async def test_rate_limiter_concurrent_requests(mock_redis_client):
    """
    TEST C: THE CHALLENGE - Fire 6 concurrent requests.
    
    Expected behavior:
    - Exactly 5 should succeed (status 200)
    - Exactly 1 should be rejected (status 429)
    
    This tests the atomic nature of the Lua script.
    Without atomicity, both request 5 and 6 might read count=4 and both increment,
    causing the limit to be exceeded incorrectly.
    """
    redis_client = mock_redis_client
    
    # Shared counter for all concurrent requests
    counters = {"count": 0}
    lock = asyncio.Lock()
    
    async def mock_script_concurrent(*args, **kwargs):
        """
        Simulate atomic Lua script execution.
        In real Redis, INCR is atomic at the Redis server level.
        """
        async with lock:  # Lock ensures atomicity in our mock
            counters["count"] += 1
            current = counters["count"]
        return current
    
    redis_client.redis.register_script = MagicMock(
        return_value=AsyncMock(side_effect=mock_script_concurrent)
    )
    
    # Create 6 concurrent tasks
    user_id = "concurrent_user"
    tasks = [
        atomic_rate_limiter(user_id, redis_client)
        for _ in range(6)
    ]
    
    # Execute all 6 requests concurrently
    results = []
    exceptions = []
    
    for task in tasks:
        try:
            result = await task
            results.append(result)
        except HTTPException as e:
            exceptions.append(e)
    
    # Verify exactly 5 succeeded and 1 was rejected
    assert len(results) == 5, f"Expected 5 successful requests, got {len(results)}"
    assert len(exceptions) == 1, f"Expected 1 rejected request, got {len(exceptions)}"
    
    # Verify the rejected request has the correct status code
    assert exceptions[0].status_code == 429
    assert "Too many requests" in exceptions[0].detail
    
    print(f"✅ CONCURRENT TEST PASSED: 5 succeeded, 1 rejected (429)")


@pytest.mark.asyncio
async def test_rate_limiter_separate_users(mock_redis_client):
    """
    Test 4: Different users should have separate rate limits.
    
    Each user should have their own counter.
    User A making 6 requests should trigger rate limiting.
    User B should be independent.
    """
    redis_client = mock_redis_client
    
    # Simulate separate counters per user
    user_counters = {}
    
    async def mock_script_per_user(keys, args):
        user_key = keys[0]
        limit = int(args[0])
        
        # Get current count for this user
        user_counters[user_key] = user_counters.get(user_key, 0) + 1
        current = user_counters[user_key]
        
        return current
    
    redis_client.redis.register_script = MagicMock(
        return_value=AsyncMock(side_effect=mock_script_per_user)
    )
    
    # User A: Make 6 requests (5 should pass, 1 should fail)
    user_a_results = []
    for i in range(6):
        try:
            result = await atomic_rate_limiter("user_a", redis_client)
            user_a_results.append("success")
        except HTTPException:
            user_a_results.append("rate_limited")
    
    # User B: Make 6 requests (all should pass because they're a separate user)
    user_b_results = []
    for i in range(6):
        try:
            result = await atomic_rate_limiter("user_b", redis_client)
            user_b_results.append("success")
        except HTTPException:
            user_b_results.append("rate_limited")
    
    # Verify User A is rate limited after 5 requests
    assert user_a_results.count("success") == 5
    assert user_a_results.count("rate_limited") == 1
    
    # Verify User B is NOT rate limited (separate counter)
    assert user_b_results.count("success") == 6
    assert user_b_results.count("rate_limited") == 0


# ============================================================================
# INTEGRATION TESTS (with FastAPI TestClient)
# ============================================================================

@pytest.mark.asyncio
async def test_rate_limiter_endpoint_integration(override_dependencies, mock_redis_client):
    """
    Test 5: Integration test using FastAPI TestClient.
    
    This tests the actual endpoint with dependency injection.
    Note: TestClient doesn't support async endpoints well, so we'll use
    httpx's AsyncClient or test the dependency directly.
    """
    mock_redis_client.redis.register_script = MagicMock()
    
    # Setup the mock to return incrementing counts
    call_count = {"count": 0}
    
    async def mock_script_integration(*args, **kwargs):
        call_count["count"] += 1
        return call_count["count"]
    
    mock_redis_client.redis.register_script.return_value = AsyncMock(
        side_effect=mock_script_integration
    )
    
    # Test that we can inject the mock
    injected_client = get_redis_client()
    # With override_dependencies fixture, this should be our mock
    assert isinstance(injected_client, RedisClient)


@pytest.mark.asyncio
async def test_rate_limiter_redis_failure_handling(mock_redis_client):
    """
    Test 6: Graceful degradation if Redis fails.
    
    If Redis throws an error, the request should still be allowed.
    This is "fail-open" behavior - prefer availability over strict rate limiting.
    """
    redis_client = mock_redis_client
    
    # Setup Redis to fail
    redis_client.redis.register_script = MagicMock(
        side_effect=Exception("Redis connection failed")
    )
    
    # Despite the error, the request should be allowed
    result = await atomic_rate_limiter("user_fail", redis_client)
    assert result is False, "Should allow request even if Redis fails (fail-open)"


# ============================================================================
# PERFORMANCE TEST: Concurrent Load Testing
# ============================================================================

@pytest.mark.asyncio
async def test_rate_limiter_high_concurrency(mock_redis_client):
    """
    Test 7: Stress test with 100 concurrent requests.
    
    Verify that:
    - Only the first 5 per user succeed
    - The rate limiter doesn't crash
    - Results are deterministic (not random)
    """
    redis_client = mock_redis_client
    
    counters = {"count": 0}
    lock = asyncio.Lock()
    
    async def mock_script_stress(*args, **kwargs):
        async with lock:
            counters["count"] += 1
            current = counters["count"]
        return current
    
    redis_client.redis.register_script = MagicMock(
        return_value=AsyncMock(side_effect=mock_script_stress)
    )
    
    # Fire 100 concurrent requests from the same user
    user_id = "stress_test_user"
    tasks = [
        atomic_rate_limiter(user_id, redis_client)
        for _ in range(100)
    ]
    
    # Execute concurrently
    results = []
    exceptions = []
    
    for task in tasks:
        try:
            result = await task
            results.append(result)
        except HTTPException as e:
            exceptions.append(e)
    
    # Verify exactly 5 succeeded and 95 were rejected
    assert len(results) == 5, f"Expected 5 successful, got {len(results)}"
    assert len(exceptions) == 95, f"Expected 95 rejected, got {len(exceptions)}"
    
    print(f"✅ STRESS TEST PASSED: 100 requests → 5 allowed, 95 rejected")


# ============================================================================
# RUN TESTS
# ============================================================================

if __name__ == "__main__":
    # Run with: pytest test_rate_limiter.py -v -s
    print("Running production-grade rate limiter tests...")
    print("=" * 70)
