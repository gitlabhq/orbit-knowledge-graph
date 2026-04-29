package com.project.access;

import static java.time.Clock;
import java.net.http.HttpClient;
import java.util.logging.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Disposable {
    String value() default "";
    int count() default 0;
}

public class Time {
    public static final Clock utcClock = Clock.systemUTC();
}

public record Project(String absolutePath, String name) {
    public static Project default() {
        return new Project("~/", "default-project");
    }

    public String display() {
        return "[" + absolutePath + "] " + name;
    }
}

public class Constants {
    public static final String BASE_URL = "localhost:8000";
}

public enum AccessResult {
    UNKNOWN_PROJECT("Unknown project"),
    ACCESS_EXPIRED("Access expired"),
    ACCESS_OK("Access ok");

    private final String message;

    AccessResult(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}

public interface IProjectAccessService {
    AccessResult validateAccess(String target);
    void revokeAccess(String target);
}

@Disposable
public class ProjectAccessService implements IProjectAccessService {
    private final Project project;
    private final Logger logger = Logger.getLogger(ProjectAccessService.class.getName());
    private final Clock clock = Time.utcClock;

    public ProjectAccessService(Project project) {
        this.project = project;
    }

    public ProjectAccessService() {
        this(Project.default());
    }

    @Override
    public AccessResult validateAccess(String target) {
        String requestUrl = Constants.BASE_URL + "/access/" + project.name + "?target=" + target + "&time=" + clock.instant();
        // Implementation would go here
        return AccessResult.ACCESS_OK;
    }

    @Override
    public void revokeAccess(String target) {
        String requestUrl = Constants.BASE_URL + "/access/" + project.name + "/revoke";
        // Implementation would go here
    }
}

public record Person(String name, int age) {
    public Person {
        if (age < 0) {
            throw new IllegalArgumentException("Age cannot be negative");
        }
    }

    public String getDisplayName() {
        return name + " (" + age + ")";
    }
}

public class Main {
    private BiFunction<String, Integer, Void> printServiceUrl = (u, p) -> { System.out.println("Service url: " + u + " and port " + p) };

    public static void main(String[] args) {
        Project project = new Project("~/project", "sample");
        System.out.println("Loaded " + project.display() + ".");

        String[] urlParts = Constants.BASE_URL.split(":");
        String url = urlParts[0];
        String port = urlParts[1];
        printServiceUrl(url, port);

        ProjectAccessService service = new ProjectAccessService(project);
        System.out.println(service.validateAccess(Project.default().name));
    }
} 
