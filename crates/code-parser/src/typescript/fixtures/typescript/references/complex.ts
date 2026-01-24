// eslint-disable-next-line @typescript-eslint/no-unused-vars
// @ts-nocheck
/* eslint-disable */

import { EventEmitter } from 'events';
import { Logger, LogLevel } from './utils/logger';
import { ApiClient, HttpMethod } from './api/client';

// Global helper functions for testing reference resolution
function generateUniqueId(prefix: string = 'id'): string {
    return `${prefix}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
}

function formatTimestamp(date: Date): string {
    return date.toISOString().replace(/[:.]/g, '-');
}

function validateEmail(email: string): boolean {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
}

function sanitizeString(input: string): string {
    return input.replace(/[<>"/\\&]/g, '');
}

function createDelay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function calculateExponentialBackoff(attempt: number, baseDelay: number = 1000): number {
    return Math.min(baseDelay * Math.pow(2, attempt), 30000);
}

function isHighPriority(priority: Priority): boolean {
    return priority >= Priority.HIGH;
}

function logWithContext(logger: Logger, level: LogLevel, message: string, context?: any): void {
    const contextStr = context ? ` [Context: ${JSON.stringify(context)}]` : '';
    logger.log(level, `${message}${contextStr}`);
}

function createSafeNotification<T>(
    type: NotificationType, 
    payload: T, 
    priority: Priority = Priority.MEDIUM
): INotification<T> {
    return {
        id: generateUniqueId('safe'),
        type,
        payload,
        timestamp: new Date(),
        priority: isHighPriority(priority) ? priority : Priority.MEDIUM
    };
}

async function processWithRetryAndLogging<T>(
    processor: NotificationProcessor,
    notification: INotification<T>,
    logger: Logger,
    maxAttempts: number = 3
): Promise<void> {
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
        try {
            logWithContext(logger, LogLevel.DEBUG, `Processing attempt ${attempt + 1}`, {
                notificationId: notification.id,
                attempt: attempt + 1
            });
            
            await processor.queueNotification(notification);
            return;
        } catch (error) {
            const delay = calculateExponentialBackoff(attempt);
            logWithContext(logger, LogLevel.WARN, `Attempt ${attempt + 1} failed, retrying in ${delay}ms`, {
                error: error.message,
                notificationId: notification.id
            });
            
            if (attempt < maxAttempts - 1) {
                await createDelay(delay);
            }
        }
    }
    
    throw new Error(`Failed to process notification ${notification.id} after ${maxAttempts} attempts`);
}

// Generic interfaces with complex type relationships
interface INotification<T = any> {
    id: string;
    type: NotificationType;
    payload: T;
    timestamp: Date;
    priority: Priority;
}

interface INotificationHandler<T> {
    canHandle(notification: INotification<T>): boolean;
    handle(notification: INotification<T>): Promise<void>;
}

// Enums and type definitions
enum NotificationType {
    EMAIL = 'email',
    SMS = 'sms',
    PUSH = 'push',
    WEBHOOK = 'webhook'
}

enum Priority {
    LOW = 1,
    MEDIUM = 2,
    HIGH = 3,
    CRITICAL = 4
}

// Abstract base class with template method pattern
abstract class BaseNotificationHandler<T> implements INotificationHandler<T> {
    protected logger: Logger;
    protected apiClient: ApiClient;
    
    constructor(logger: Logger, apiClient: ApiClient) {
        this.logger = logger;
        this.apiClient = apiClient;
    }
    
    abstract canHandle(notification: INotification<T>): boolean;
    
    async handle(notification: INotification<T>): Promise<void> {
        this.logger.log(LogLevel.INFO, `Processing notification ${notification.id}`);
        
        try {
            await this.preProcess(notification);
            await this.doHandle(notification);
            await this.postProcess(notification);
        } catch (error) {
            this.logger.log(LogLevel.ERROR, `Failed to process notification: ${error.message}`);
            throw error;
        }
    }
    
    protected async preProcess(notification: INotification<T>): Promise<void> {
        // Default implementation
        this.logger.log(LogLevel.DEBUG, `Pre-processing notification ${notification.id}`);
    }
    
    protected abstract doHandle(notification: INotification<T>): Promise<void>;
    
    protected async postProcess(notification: INotification<T>): Promise<void> {
        // Default implementation  
        this.logger.log(LogLevel.DEBUG, `Post-processing notification ${notification.id}`);
    }
}

// Concrete implementations with method overrides
class EmailNotificationHandler extends BaseNotificationHandler<EmailPayload> {
    private emailService: EmailService;
    
    constructor(logger: Logger, apiClient: ApiClient, emailService: EmailService) {
        super(logger, apiClient);
        this.emailService = emailService;
    }
    
    canHandle(notification: INotification<EmailPayload>): boolean {
        return notification.type === NotificationType.EMAIL;
    }
    
    protected async doHandle(notification: INotification<EmailPayload>): Promise<void> {
        const { to, subject, body } = notification.payload;
        
        if (!validateEmail(to)) {
            throw new Error(`Invalid email address: ${to}`);
        }
        
        const sanitizedSubject = sanitizeString(subject);
        const sanitizedBody = sanitizeString(body);
        
        await this.emailService.sendEmail(to, sanitizedSubject, sanitizedBody);
        
        // Log using inherited logger with context
        logWithContext(this.logger, LogLevel.INFO, `Email sent to ${to}`, {
            notificationId: notification.id,
            subject: sanitizedSubject
        });
    }
    
    protected async postProcess(notification: INotification<EmailPayload>): Promise<void> {
        await super.postProcess(notification);
        
        // Additional email-specific post-processing
        await this.apiClient.request({
            method: HttpMethod.POST,
            url: '/notifications/email/delivered',
            data: { notificationId: notification.id }
        });
    }
}

// Factory pattern with dependency injection
class NotificationHandlerFactory {
    private logger: Logger;
    private apiClient: ApiClient;
    private emailService: EmailService;
    private smsService: SmsService;
    
    constructor(
        logger: Logger, 
        apiClient: ApiClient, 
        emailService: EmailService, 
        smsService: SmsService
    ) {
        this.logger = logger;
        this.apiClient = apiClient;
        this.emailService = emailService;
        this.smsService = smsService;
    }
    
    createHandler<T>(type: NotificationType): INotificationHandler<T> | null {
        switch (type) {
            case NotificationType.EMAIL:
                return new EmailNotificationHandler(
                    this.logger, 
                    this.apiClient, 
                    this.emailService
                ) as INotificationHandler<T>;
            case NotificationType.SMS:
                return new SmsNotificationHandler(
                    this.logger, 
                    this.apiClient, 
                    this.smsService
                ) as INotificationHandler<T>;
            default:
                this.logger.log(LogLevel.WARN, `No handler available for type: ${type}`);
                return null;
        }
    }
}

// Event-driven architecture with observer pattern
class NotificationProcessor extends EventEmitter {
    private handlerFactory: NotificationHandlerFactory;
    private processingQueue: Map<string, INotification>;
    private retryAttempts: Map<string, number>;
    private maxRetries: number = 3;
    
    constructor(handlerFactory: NotificationHandlerFactory) {
        super();
        this.handlerFactory = handlerFactory;
        this.processingQueue = new Map();
        this.retryAttempts = new Map();
        
        // Set up event listeners with complex closure capturing
        this.setupEventListeners();
    }
    
    private setupEventListeners(): void {
        // Complex closure that captures 'this' and other variables
        const processWithRetry = async (notification: INotification) => {
            const attempts = this.retryAttempts.get(notification.id) || 0;
            
            if (attempts >= this.maxRetries) {
                this.emit('notification:failed', notification, 'Max retries exceeded');
                this.processingQueue.delete(notification.id);
                this.retryAttempts.delete(notification.id);
                return;
            }
            
            try {
                await this.processNotification(notification);
                this.emit('notification:success', notification);
                this.processingQueue.delete(notification.id);
                this.retryAttempts.delete(notification.id);
            } catch (error) {
                this.retryAttempts.set(notification.id, attempts + 1);
                
                // Exponential backoff with closure
                const delay = calculateExponentialBackoff(attempts);
                setTimeout(() => {
                    processWithRetry(notification);
                }, delay);
            }
        };
        
        this.on('notification:queue', processWithRetry);
        
        // More event handlers with different patterns
        this.on('notification:priority', (notification: INotification) => {
            if (isHighPriority(notification.priority)) {
                // Priority notifications bypass queue
                this.processNotification(notification)
                    .then(() => this.emit('notification:success', notification))
                    .catch(error => this.emit('notification:failed', notification, error.message));
            } else {
                this.emit('notification:queue', notification);
            }
        });
    }
    
    async queueNotification<T>(notification: INotification<T>): Promise<void> {
        this.processingQueue.set(notification.id, notification);
        this.emit('notification:priority', notification);
    }
    
    private async processNotification<T>(notification: INotification<T>): Promise<void> {
        const handler = this.handlerFactory.createHandler<T>(notification.type);
        
        if (!handler) {
            throw new Error(`No handler found for notification type: ${notification.type}`);
        }
        
        if (!handler.canHandle(notification)) {
            throw new Error(`Handler cannot process notification ${notification.id}`);
        }
        
        await handler.handle(notification);
    }
    
    // Method chaining pattern
    withMaxRetries(maxRetries: number): NotificationProcessor {
        this.maxRetries = maxRetries;
        return this;
    }
    
    // Fluent interface with complex method chaining
    createNotificationBuilder(): NotificationBuilder {
        return new NotificationBuilder(this);
    }
}

// Builder pattern with fluent interface
class NotificationBuilder {
    private processor: NotificationProcessor;
    private notification: Partial<INotification> = {};
    
    constructor(processor: NotificationProcessor) {
        this.processor = processor;
        this.notification.id = generateUniqueId('notification');
        this.notification.timestamp = new Date();
    }
    
    withType(type: NotificationType): NotificationBuilder {
        this.notification.type = type;
        return this;
    }
    
    withPayload<T>(payload: T): NotificationBuilder {
        this.notification.payload = payload;
        return this;
    }
    
    withPriority(priority: Priority): NotificationBuilder {
        this.notification.priority = priority;
        return this;
    }
    
    async send(): Promise<void> {
        if (!this.isValid()) {
            throw new Error('Invalid notification configuration');
        }
        
        await this.processor.queueNotification(this.notification as INotification);
    }
    
    private isValid(): boolean {
        return !!(this.notification.type && 
                 this.notification.payload && 
                 this.notification.priority !== undefined);
    }
}

// Usage with complex interactions and reference patterns
async function demonstrateComplexUsage(): Promise<void> {
    const logger = new Logger();
    const apiClient = new ApiClient('https://api.example.com');
    const emailService = new EmailService();
    const smsService = new SmsService();
    
    // Dependency injection
    const handlerFactory = new NotificationHandlerFactory(
        logger, 
        apiClient, 
        emailService, 
        smsService
    );
    
    const processor = new NotificationProcessor(handlerFactory)
        .withMaxRetries(5);
    
    // Event listener with complex closure
    processor.on('notification:success', (notification: INotification) => {
        logger.log(LogLevel.INFO, `Successfully processed ${notification.id}`);
    });
    
    processor.on('notification:failed', (notification: INotification, reason: string) => {
        logger.log(LogLevel.ERROR, `Failed to process ${notification.id}: ${reason}`);
    });
    
    // Builder pattern usage
    await processor
        .createNotificationBuilder()
        .withType(NotificationType.EMAIL)
        .withPayload({
            to: 'user@example.com',
            subject: 'Welcome!',
            body: 'Thank you for signing up'
        })
        .withPriority(Priority.HIGH)
        .send();
    
    // Direct usage with variable references across scopes
    const criticalNotification: INotification<EmailPayload> = {
        id: generateUniqueId('critical'),
        type: NotificationType.EMAIL,
        payload: {
            to: 'admin@example.com',
            subject: sanitizeString('System Alert'),
            body: sanitizeString('Critical system issue detected')
        },
        timestamp: new Date(),
        priority: Priority.CRITICAL
    };
    
    // Add delay before processing critical notification
    await createDelay(100);
    await processor.queueNotification(criticalNotification);
    
    // Log the operation with context
    logWithContext(logger, LogLevel.INFO, 'Critical notification queued', {
        notificationId: criticalNotification.id,
        timestamp: formatTimestamp(criticalNotification.timestamp)
    });
    
    // Test the new helper functions with more complex patterns
    const safeEmailNotification = createSafeNotification(
        NotificationType.EMAIL,
        {
            to: 'test@example.com',
            subject: sanitizeString('Test Subject'),
            body: sanitizeString('Test body content')
        },
        Priority.LOW
    );
    
    try {
        await processWithRetryAndLogging(processor, safeEmailNotification, logger, 2);
    } catch (error) {
        logWithContext(logger, LogLevel.ERROR, 'Failed to process safe notification', {
            error: error.message,
            notificationId: safeEmailNotification.id
        });
    }
}

// Type definitions referenced throughout
interface EmailPayload {
    to: string;
    subject: string;
    body: string;
}

// Mock classes for completeness
class EmailService {
    async sendEmail(to: string, subject: string, body: string): Promise<void> {
        console.log(`Sending email to ${to}: ${subject}`);
    }
}

class SmsService {
    async sendSms(to: string, message: string): Promise<void> {
        console.log(`Sending SMS to ${to}: ${message}`);
    }
}

class SmsNotificationHandler extends BaseNotificationHandler<SmsPayload> {
    private smsService: SmsService;
    
    constructor(logger: Logger, apiClient: ApiClient, smsService: SmsService) {
        super(logger, apiClient);
        this.smsService = smsService;
    }
    
    canHandle(notification: INotification<SmsPayload>): boolean {
        return notification.type === NotificationType.SMS;
    }
    
    protected async doHandle(notification: INotification<SmsPayload>): Promise<void> {
        const { to, message } = notification.payload;
        await this.smsService.sendSms(to, message);
    }
}

interface SmsPayload {
    to: string;
    message: string;
}
