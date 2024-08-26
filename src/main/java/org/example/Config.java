package org.example;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Config {

    @Value("${app.confluent.secrets}")
    private String confluentSecret;

    @Value("${app.confluent.bootstrapserver}")
    private String confluentBootstrapServer;

    @Bean
    public KStreamer kStreamer() {
        return new KStreamer(confluentSecret, confluentBootstrapServer);
    }
}
