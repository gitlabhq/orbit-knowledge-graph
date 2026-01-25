package com.example.userservice.controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.*;
import javax.naming.Context;
import javax.naming.InitialContext;

@RestController
@RequestMapping("/api/config")
public class ConfigController {
    
    private static final Logger logger = LogManager.getLogger(ConfigController.class);
    
    @GetMapping("/lookup")
    public String lookupConfig(@RequestParam String configName) {
        // Log user input directly (vulnerable to Log4Shell)
        logger.info("Looking up configuration: {}", configName);
        
        try {
            Context ctx = new InitialContext();
            Object config = ctx.lookup(configName);
            logger.info("Configuration found: {}", config);
            return "Configuration: " + config.toString();
        } catch (Exception e) {
            logger.error("Failed to lookup configuration: {}", configName, e);
            return "Configuration not found";
        }
    }
    
    @PostMapping("/debug")
    public String debugLog(@RequestBody String message) {
        // Direct logging of user input
        logger.debug("Debug message from user: {}", message);
        return "Debug message logged";
    }
}
