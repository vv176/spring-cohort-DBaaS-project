package com.example.order.log;

import org.ppa.LogMagickClient;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class LogConfig {

    @Bean
    public LogMagickClient logMagickClient() {
        return new LogMagickClient("orderservice");
    }

}
