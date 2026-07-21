package interview.guide.common.aspect;

import interview.guide.common.annotation.RateLimit;
import interview.guide.common.exception.RateLimitExceededException;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * 限流 AOP 切面
 * 支持可重复注解，逐条执行独立的限流规则，任一规则不通过即拒绝
 */
@Slf4j
@Aspect
@Component
@RequiredArgsConstructor
public class RateLimitAspect {

    private final RedissonClient redissonClient;

    private static final String LUA_SCRIPT;
    private String luaScriptSha;
    private RScript rScript;

    static {
        try {
            ClassPathResource resource = new ClassPathResource("scripts/rate_limit_single.lua");
            LUA_SCRIPT = new String(resource.getContentAsByteArray(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("加载限流 Lua 脚本失败", e);
        }
    }

    @jakarta.annotation.PostConstruct
    public void init() {
        // 初始化 RedisScript,把脚本加载到 Redis 中
        rScript = redissonClient.getScript(StringCodec.INSTANCE);
        loadScript();
    }

    private void loadScript() {
        this.luaScriptSha = rScript.scriptLoad(LUA_SCRIPT);
        log.info("限流 Lua 脚本加载完成, SHA1: {}", luaScriptSha);
    }

    /**
     * 环绕通知：拦截带 @RateLimit 或 @RateLimit.Container 注解的方法
     */
    @Around("@annotation(interview.guide.common.annotation.RateLimit) || " +
            "@annotation(interview.guide.common.annotation.RateLimit.Container)")
    public Object around(ProceedingJoinPoint joinPoint) throws Throwable {
        //  获取方法信息
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        String className = method.getDeclaringClass().getSimpleName();
        String methodName = method.getName();

        // 获取限流规则
        RateLimit[] rules = method.getAnnotationsByType(RateLimit.class);
        long nowMs = System.currentTimeMillis();
        String requestId = UUID.randomUUID().toString();

        // 逐条执行限流规则
        for (RateLimit rule : rules) {
            // 3a: 计算时间窗口（毫秒）
            long intervalMs = calculateIntervalMs(rule.interval(), rule.timeUnit());

            // 3b: 生成 Redis Key
            String key = generateKey(className, methodName, rule.dimension());

            // 3c: 执行 Lua 脚本进行限流判断
            Long result = executeRateLimitScript(key, nowMs, requestId, intervalMs, rule.count());

            // 3d: 如果限流失败（result == 0），立即拒绝
            if (result == null || result == 0) {
                return handleRateLimitExceeded(joinPoint, rule, key);
            }
        }

        // 4: 放行
        return joinPoint.proceed();
    }

    //
    private Long executeRateLimitScript(String key, long nowMs, String requestId, long intervalMs, double count) {
        List<Object> keysList = Collections.singletonList(key);
        Object[] args = {
                String.valueOf(nowMs),       // ARGV[1]: 当前时间戳
                String.valueOf(1),         // ARGV[2]: 申请令牌数（固定为1）
                String.valueOf(intervalMs),  // ARGV[3]: 时间窗口
                String.valueOf(count),       // ARGV[4]: 最大令牌数
                requestId                    // ARGV[5]: 请求ID
        };

        try {
            // 执行 Lua 脚本（高效）
            Object resultObj = rScript.evalSha(
                    RScript.Mode.READ_WRITE,
                    luaScriptSha,
                    RScript.ReturnType.VALUE,
                    keysList,
                    args
            );
            return convertToLong(resultObj);
        } catch (org.redisson.client.RedisException e) {
            // Redis 重启后脚本缓存丢失，重新加载并重试
            if (e.getMessage() != null && e.getMessage().contains("NOSCRIPT")) {
                loadScript();
                Object resultObj = rScript.evalSha(
                        RScript.Mode.READ_WRITE,
                        luaScriptSha,
                        RScript.ReturnType.VALUE,
                        keysList,
                        args
                );
                return convertToLong(resultObj);
            }
            throw e;
        }
    }

    // 时间单位转换
    private long calculateIntervalMs(long interval, RateLimit.TimeUnit unit) {
        return switch (unit) {
            case MILLISECONDS -> interval;
            case SECONDS -> interval * 1000;
            case MINUTES -> interval * 60 * 1000;
            case HOURS -> interval * 3600 * 1000;
            case DAYS -> interval * 86400 * 1000;
        };
    }

    private Long convertToLong(Object obj) {
        if (obj instanceof Number n) {
            return n.longValue();
        }
        if (obj instanceof String s) {
            try {
                return Long.parseLong(s);
            } catch (NumberFormatException e) {
                log.warn("无法将字符串转换为Long: {}", obj);
                return null;
            }
        }
        log.warn("不支持的对象类型转换为Long: {}", obj != null ? obj.getClass().getName() : "null");
        return null;
    }

    private String generateKey(String className, String methodName, RateLimit.Dimension dimension) {
        //hashTag决定分片位置,保证同一方法的不同维度限流数据在同一分片
        String hashTag = "{" + className + ":" + methodName + "}";
        String keyPrefix = "ratelimit:" + hashTag;

        return switch (dimension) {
            case GLOBAL -> keyPrefix + ":global";
            case IP -> keyPrefix + ":ip:" + getClientIp();
            case USER -> keyPrefix + ":user:" + getCurrentUserId();
        };
    }

    private Object handleRateLimitExceeded(ProceedingJoinPoint joinPoint, RateLimit rateLimit, String key)
            throws Throwable {
        String methodName = joinPoint.getSignature().getName();

        if (rateLimit.fallback() != null && !rateLimit.fallback().isEmpty()) {
            try {
                Method fallbackMethod = findFallbackMethod(joinPoint, rateLimit.fallback());
                if (fallbackMethod != null) {
                    log.debug("限流触发，执行降级方法: {}.{} -> {}",
                            joinPoint.getTarget().getClass().getSimpleName(),
                            methodName,
                            rateLimit.fallback());
                    if (fallbackMethod.getParameterCount() > 0) {
                        return fallbackMethod.invoke(joinPoint.getTarget(), joinPoint.getArgs());
                    } else {
                        return fallbackMethod.invoke(joinPoint.getTarget());
                    }
                }
            } catch (Exception e) {
                log.error("降级方法执行失败: {}", rateLimit.fallback(), e);
            }
        }

        log.debug("限流触发，拒绝请求: key={}, count={} per {} {}",
                key, rateLimit.count(), rateLimit.interval(), rateLimit.timeUnit());
        throw new RateLimitExceededException("请求过于频繁，请稍后再试");
    }

    private Method findFallbackMethod(ProceedingJoinPoint joinPoint, String fallbackName) {
        Class<?> targetClass = joinPoint.getTarget().getClass();
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Class<?>[] parameterTypes = signature.getParameterTypes();

        try {
            Method method = targetClass.getDeclaredMethod(fallbackName, parameterTypes);
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException e) {
            try {
                Method method = targetClass.getDeclaredMethod(fallbackName);
                method.setAccessible(true);
                return method;
            } catch (NoSuchMethodException ex) {
                log.warn("未找到降级方法: {}.{} (需无参或参数列表一致)",
                        targetClass.getSimpleName(), fallbackName);
                return null;
            }
        }
    }

    private String getClientIp() {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attributes == null) {
            return "unknown";
        }

        HttpServletRequest request = attributes.getRequest();
        String ip = request.getHeader("X-Forwarded-For");

        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("X-Real-IP");
        }
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("WL-Proxy-Client-IP");
        }
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }

        if (ip != null && ip.contains(",")) {
            ip = ip.split(",")[0].trim();
        }

        return ip != null ? ip : "unknown";
    }

    private String getCurrentUserId() {
        // 从请求中获取当前用户ID,attributes是请求属性
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attributes == null) {
            return "anonymous";
        }

        HttpServletRequest request = attributes.getRequest();

        Object userId = request.getAttribute("userId");
        if (userId != null) {
            return userId.toString();
        }

        userId = request.getHeader("X-User-Id");
        if (userId != null) {
            return userId.toString();
        }

        return "anonymous";
    }
}
