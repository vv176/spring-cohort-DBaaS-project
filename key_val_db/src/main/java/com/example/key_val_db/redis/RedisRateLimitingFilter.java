package com.example.key_val_db.redis;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

@Component
@Order(1)
public class RedisRateLimitingFilter extends OncePerRequestFilter {

    @Autowired
    private SlidingWindowBasedRedisRateLimiter rateLimiter;
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        String requestUri = request.getRequestURI();
        String[] pathParts = requestUri.split("/");

        if (pathParts.length < 3) { // Ensure the client ID exists in the path
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid request URL. Missing client ID.");
            return;
        }

        String clientId = pathParts[3]; // Assuming /db/{client}/... (client is at index 2)

        if (!rateLimiter.allowRequest(clientId)) {
            response.sendError(429, "Rate limit exceeded");
            return;
        }

        filterChain.doFilter(request, response);
    }
}
