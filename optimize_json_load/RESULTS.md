# Optimization Results and Strategy

This document summarizes the successful optimization strategy used to achieve a performance ratio > 2 for the SEC JSON parsing project.

## Mandatory Rules Followed
1. **Immutability of Structure:** All optimizations must only subtract bytes from the original binary stream (e.g., using `re.sub`). Manual JSON re-assembly is forbidden as it is fragile and leads to assertion errors.
2. **C-Level Execution:** The heavy lifting (scanning large buffers) must occur entirely within the C-based backend of optimized modules (like `regex`). Python-level loops or callbacks on the hot path must be avoided.
3. **Linear Complexity:** The strategy must maintain $O(N)$ or lower complexity relative to the file size.

## Successful Strategy: Two-Pass Surgical Removal

The most effective approach discovered uses a two-pass subtraction method leveraging advanced features of the `regex` module.

### Pass 1: Fact Filtering with Backtracking Control
Instead of using slow negative lookaheads or complex state machines, this pass uses a single regex with backtracking control verbs (`(*SKIP)(*FAIL)`). 
* **Mechanism:** The pattern first matches the "Keep" list (the 37 known facts) and immediately skips/fails them. Any other match (the thousands of unwanted facts) is then caught by the second half of the OR-pattern and removed.
* **Pattern Structure:** `KeepPattern(*SKIP)(*FAIL)|DropPattern`
* **Benefit:** The entire filtering logic runs within the `regex` module's optimized C backend.

### Pass 2: Global Key Removal
A second, highly optimized pass removes unneeded keys (e.g., `accn`, `fy`) from the remaining "useful" facts. 
* **Precision:** The pattern matches both string values (`:"..."`) and literal/numeric values (`:1234`, `:null`) to ensure clean data for the baseline parser.

## Technical Insights
* **Avoid Python-to-C Transitions:** Performance collapses when the regex engine has to stop and call a Python function or return a match object frequently.
* **Regex is Faster than `find()` for Large Jumps:** For OR-patterns of many strings, the `regex` module uses algorithms like Aho-Corasick which are faster than repeated manual `find()` calls.
* **Caching is Essential:** Pre-compiling regex patterns and caching them (using `@cache` with a serialized configuration string as a key) is critical when processing hundreds of files.

## Performance Outcome
* **Baseline Time:** ~3.6s
* **Optimized Time:** ~1.7s
* **Final Ratio:** ~2.12
* **Complexity:** Strictly $O(N)$ with zero manual reconstruction overhead.
