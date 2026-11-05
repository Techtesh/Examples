# Example file to setup agentic harnesses
import asyncio, random, time 
from dataclasses import dataclass 

# ---------- Provided: do not modify ---------- 

class TransientError(Exception): ...  

class RateLimitError(TransientError): ...  

class ProviderUnavailable(TransientError): ...  

class InvalidRequestError(Exception): ... 

class MockProvider:  

    def init(self, 
        name, 
        fail_rate=0.3, 
        latency=(0.1, 1.5), 
        max_concurrency=5): 			
        self.name, self.fail_rate, self.latency = name, fail_rate, latency 				
        self.max_concurrency = max_concurrency 
        self._in_flight = 0 

    self.peak_in_flight = 0 

    async def generate(self, prompt: str) -> str: 
        if not prompt.strip(): 
            raise InvalidRequestError("empty prompt") 
        self._in_flight += 1 
        self.peak_in_flight = max(self.peak_in_flight, self._in_flight) 
        try: 
            if self._in_flight > self.max_concurrency: 
                raise RateLimitError(f"{self.name}: concurrency exceeded") 
            await asyncio.sleep(random.uniform(*self.latency)) 
            r = random.random() 
            if r < self.fail_rate / 2: 
                raise RateLimitError(f"{self.name}: 429") 
            if r < self.fail_rate: 
                raise ProviderUnavailable(f"{self.name}: 503") 
            return f"[{self.name}] answer to: {prompt[:30]}" 
        finally: 
            self._in_flight -= 1 
  

@dataclass  

class Result:  

    text: str  
    provider: str  
    attempts: int  
    latency_ms: float 

class AllProvidersFailed(Exception):  

    def init(self, errors): 

        super().init(f"all providers failed: {errors}")  

        self.errors = errors 
# ---------- Your code ---------- 
class LLMGateway:
    def __init__(self, providers, timeout_s=1.0, max_retries=2, base_backoff_s=0.1):
        if timeout_s <= 0 or base_backoff_s < 0:
            raise ValueError("timeout_s must be positive; backoff must be nonnegative")
        if not isinstance(max_retries, int) or max_retries < 0:
            raise ValueError("max_retries must be a nonnegative integer")
        self.providers = list(providers)
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.base_backoff_s = base_backoff_s
        self._states = {}
        for provider in self.providers:
            if provider.max_concurrency < 1:
                raise ValueError("provider.max_concurrency must be positive")
            self._states[provider] = _ProviderState(
                asyncio.Semaphore(provider.max_concurrency)
            )

    async def complete(self, prompt: str) -> Result:
        started = time.perf_counter()
        attempts = 0
        errors = []

        for provider in self.providers:
            state = self._states[provider]
            for retry in range(self.max_retries + 1):
                # Check after acquiring: the breaker may open while we queue.
                async with state.semaphore:
                    now = time.monotonic()
                    if state.open_until > now:
                        errors.append({
                            "provider": provider.name,
                            "attempt": None,
                            "error": "circuit open",
                        })
                        break
                    if state.open_until:
                        state.open_until = 0.0
                        state.failures = 0

                    generation = state.generation
                    attempts += 1
                    try:
                        text = await asyncio.wait_for(
                            provider.generate(prompt), timeout=self.timeout_s
                        )
                    except InvalidRequestError:
                        raise
                    except (TransientError, asyncio.TimeoutError) as exc:
                        errors.append({
                            "provider": provider.name,
                            "attempt": retry + 1,
                            "error": exc,
                        })
                        # Ignore completions from before the last breaker trip.
                        if generation == state.generation:
                            state.failures += 1
                            if state.failures >= 5:
                                state.open_until = time.monotonic() + 10.0
                                state.generation += 1
                    else:
                        if generation == state.generation:
                            state.failures = 0
                        return Result(
                            text=text,
                            provider=provider.name,
                            attempts=attempts,
                            latency_ms=(time.perf_counter() - started) * 1000,
                        )

                if state.open_until > time.monotonic():
                    break
                if retry < self.max_retries:
                    ceiling = self.base_backoff_s * (2 ** retry)
                    await asyncio.sleep(random.uniform(0, ceiling))

        raise AllProvidersFailed(errors)


# Production: add bounded admission queues and an overall request deadline.
# Add metrics/tracing, Retry-After handling, and provider circuit breakers.
# Ensure provider clients support cancellation and enforce network timeouts.

 

# ---------- Harness: do not modify ----------  

async def main(): 
    primary = MockProvider( 
        "primary", 
        fail_rate=0.4, 
        latency=(0.1, 1.5), 
        max_concurrency=5 
    ) 
 
    fallback = MockProvider( 
        "fallback", 
        fail_rate=0.1, 
        latency=(0.2, 0.6), 
        max_concurrency=10 
    ) 
 
    gw = LLMGateway([primary, fallback]) 
 
    prompts = [f"question {i}" for i in range(50)] + [""] 
 
    results = await asyncio.gather( 
        *(gw.complete(p) for p in prompts), 
        return_exceptions=True 
    ) 
 
    ok = [r for r in results if isinstance(r, Result)] 
 
    print(f"success: {len(ok)}/{len(prompts)}") 
 
    print( 
        "by provider:", 
        { 
            p: sum(r.provider == p for r in ok) 
            for p in ("primary", "fallback") 
        } 
    ) 
 
    print( 
        "errors:", 
        [ 
            type(r).__name__ 
            for r in results 
            if isinstance(r, Exception) 
        ] 
    ) 
 
    print( 
        "peak in-flight:", 
        primary.peak_in_flight, 
        fallback.peak_in_flight 
    ) 
 
    assert ( 
        primary.peak_in_flight <= 5 
        and fallback.peak_in_flight <= 10 
    ), "concurrency cap violated" 
 
 
asyncio.run(main()) 

 

 
