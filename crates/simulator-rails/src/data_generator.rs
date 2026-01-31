use chrono::Utc;
use fake::faker::company::en::*;
use fake::faker::name::en::*;
use fake::Fake;
use rand::seq::SliceRandom;
use rand::Rng;
use uuid::Uuid;

const JAVA_CLASS_TYPES: &[&str] = &[
    "Service",
    "Controller",
    "Repository",
    "Model",
    "Validator",
    "Handler",
    "Mapper",
    "Factory",
    "Builder",
];

const JAVA_CLASS_PREFIXES: &[&str] = &[
    "User",
    "Order",
    "Product",
    "Customer",
    "Payment",
    "Invoice",
    "Shipping",
    "Inventory",
    "Account",
    "Transaction",
];

const JAVA_PACKAGES: &[&str] = &[
    "service",
    "controller",
    "repository",
    "model",
    "util",
    "config",
    "handler",
    "mapper",
    "factory",
];

const JAVA_IMPORTS: &[&str] = &[
    "java.util.List",
    "java.util.Optional",
    "java.util.ArrayList",
    "java.util.Map",
    "java.util.HashMap",
    "java.time.LocalDateTime",
    "java.time.LocalDate",
    "org.springframework.stereotype.Service",
    "org.springframework.stereotype.Component",
    "org.springframework.beans.factory.annotation.Autowired",
    "javax.persistence.Entity",
    "javax.persistence.Id",
    "javax.persistence.GeneratedValue",
];

const TECH_WORDS: &[&str] = &[
    "implementation",
    "service",
    "controller",
    "repository",
    "database",
    "query",
    "performance",
    "optimization",
    "caching",
    "validation",
    "authentication",
    "authorization",
    "endpoint",
    "request",
    "response",
    "data",
    "model",
    "entity",
    "transaction",
    "integration",
    "testing",
    "deployment",
    "configuration",
];

const COMMENT_TEMPLATES: &[&str] = &[
    "Looks good to me!",
    "Could we add some tests for this?",
    "Nice implementation. One minor suggestion below.",
    "This needs some refactoring before we merge.",
    "Have we considered the performance implications?",
    "The logic here is clean and easy to follow.",
    "Should we add logging for debugging purposes?",
    "This aligns well with our architectural guidelines.",
    "Can we add documentation for the public methods?",
    "Great work on this feature!",
];

pub struct DataGenerator;

impl DataGenerator {
    pub fn project_name() -> String {
        format!("load-test-project-{}", short_uuid())
    }

    pub fn project_description() -> String {
        let prefix = random_choice(JAVA_CLASS_PREFIXES).to_lowercase();
        format!("A Java-based microservice for {} management", prefix)
    }

    pub fn java_class_name() -> String {
        let mut rng = rand::thread_rng();

        // Mix of approaches for variety
        let approach = rng.gen_range(0..3);

        let prefix = match approach {
            0 => {
                // Use faker name (e.g., "Johnson", "Smith")
                let name: String = LastName().fake();
                name.chars().filter(|c| c.is_alphanumeric()).collect()
            }
            1 => {
                // Use faker buzzword (e.g., "Synergy", "Dynamic")
                let word: String = Buzzword().fake();
                to_pascal_case(&word)
            }
            _ => {
                // Use existing prefixes
                random_choice(JAVA_CLASS_PREFIXES).to_string()
            }
        };

        let class_type = random_choice(JAVA_CLASS_TYPES);
        let unique_suffix = Self::short_id();

        format!("{}{}{}", prefix, class_type, unique_suffix)
    }

    /// Generate a unique short identifier to avoid file conflicts
    pub fn short_id() -> String {
        let mut rng = rand::thread_rng();
        let chars: String = (0..4)
            .map(|_| rng.gen_range(b'a'..=b'z') as char)
            .collect();
        format!("{}{}", chars, rng.gen_range(100..999))
    }

    pub fn java_package() -> String {
        random_choice(JAVA_PACKAGES).to_string()
    }

    pub fn java_file_path(class_name: &str, package: &str) -> String {
        format!(
            "src/main/java/com/example/loadtest/{}/{}.java",
            package, class_name
        )
    }

    pub fn java_class_content(class_name: &str, package: &str) -> String {
        let mut rng = rand::thread_rng();
        let import_count = rng.gen_range(3..=6);
        let imports: Vec<&str> = JAVA_IMPORTS
            .choose_multiple(&mut rng, import_count)
            .copied()
            .collect();

        let imports_str = imports
            .iter()
            .map(|i| format!("import {};", i))
            .collect::<Vec<_>>()
            .join("\n");

        let methods = generate_random_methods(class_name);
        let timestamp = Utc::now().to_rfc3339();

        format!(
            r#"package com.example.loadtest.{package};

{imports_str}

/**
 * {class_name} - Auto-generated for load testing
 * Created at: {timestamp}
 */
public class {class_name} {{

    private Long id;
    private String name;
    private LocalDateTime createdAt;

    public {class_name}() {{
        this.createdAt = LocalDateTime.now();
    }}

    public {class_name}(Long id, String name) {{
        this.id = id;
        this.name = name;
        this.createdAt = LocalDateTime.now();
    }}

    public Long getId() {{
        return id;
    }}

    public void setId(Long id) {{
        this.id = id;
    }}

    public String getName() {{
        return name;
    }}

    public void setName(String name) {{
        this.name = name;
    }}

    public LocalDateTime getCreatedAt() {{
        return createdAt;
    }}

    public void setCreatedAt(LocalDateTime createdAt) {{
        this.createdAt = createdAt;
    }}

{methods}
}}"#
        )
    }

    pub fn pom_xml_content(project_name: &str) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.example.loadtest</groupId>
    <artifactId>{project_name}</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <packaging>jar</packaging>

    <name>{project_name}</name>
    <description>Load testing project - auto-generated</description>

    <properties>
        <java.version>17</java.version>
        <maven.compiler.source>17</maven.compiler.source>
        <maven.compiler.target>17</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
            <version>3.2.0</version>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-jpa</artifactId>
            <version>3.2.0</version>
        </dependency>
    </dependencies>
</project>"#
        )
    }

    pub fn commit_message_for_new_file(class_name: &str) -> String {
        let actions = ["Add", "Implement", "Create"];
        let action = random_choice(&actions);
        format!("{} {} class", action, class_name)
    }

    pub fn commit_message_for_update(class_name: &str) -> String {
        let actions = ["Update", "Refactor", "Fix", "Improve", "Enhance"];
        let changes = [
            "implementation",
            "null check",
            "validation logic",
            "error handling",
            "performance",
        ];
        let action = random_choice(&actions);
        let change = random_choice(&changes);
        format!("{} {} in {}", action, change, class_name)
    }

    pub fn issue_title() -> String {
        let prefixes = ["Bug:", "Feature:", "Enhancement:", "Refactor:", "Task:"];
        let subjects = [
            "Improve performance of database queries",
            "Add validation for user input",
            "Implement caching for frequently accessed data",
            "Fix null pointer exception in service layer",
            "Update dependencies to latest versions",
            "Add unit tests for repository layer",
            "Refactor controller to use DTOs",
            "Implement pagination for list endpoints",
        ];
        format!("{} {}", random_choice(&prefixes), random_choice(&subjects))
    }

    pub fn issue_description() -> String {
        format!(
            r#"## Description
{}

## Acceptance Criteria
- [ ] {}
- [ ] {}
- [ ] {}

## Technical Notes
{}"#,
            random_sentences(2),
            random_sentences(1),
            random_sentences(1),
            random_sentences(1),
            random_sentences(3)
        )
    }

    pub fn milestone_title() -> String {
        let versions = ["1.0.0", "1.1.0", "1.2.0", "2.0.0", "2.1.0"];
        let names = ["Alpha", "Beta", "Release Candidate", "GA", "Maintenance"];
        format!("v{} - {}", random_choice(&versions), random_choice(&names))
    }

    pub fn milestone_description() -> String {
        format!("Milestone for {}", random_sentences(1).to_lowercase())
    }

    pub fn merge_request_title() -> String {
        let actions = ["Add", "Update", "Fix", "Refactor", "Implement"];
        let subjects = [
            "user authentication module",
            "order processing service",
            "payment integration",
            "database migration scripts",
            "API endpoint validation",
            "error handling improvements",
            "logging configuration",
            "security enhancements",
        ];
        format!("{} {}", random_choice(&actions), random_choice(&subjects))
    }

    pub fn merge_request_description() -> String {
        format!(
            r#"## Summary
{}

## Changes
- {}
- {}
- {}

## Testing
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Manual testing completed

## Checklist
- [ ] Code follows style guidelines
- [ ] Documentation updated
- [ ] No breaking changes"#,
            random_sentences(2),
            random_sentences(1),
            random_sentences(1),
            random_sentences(1)
        )
    }

    pub fn comment_body() -> String {
        random_choice(COMMENT_TEMPLATES).to_string()
    }

    pub fn reply_body(original_comment: &str) -> String {
        let reply_starters = [
            "I agree with this.",
            "Good point!",
            "Thanks for the feedback.",
            "Let me address this.",
            "I'll look into it.",
            "That makes sense.",
            "I have a different perspective on this.",
            "This is now fixed.",
        ];
        let truncated = if original_comment.len() > 50 {
            format!("{}...", &original_comment[..50])
        } else {
            original_comment.to_string()
        };
        format!(
            "> {}\n\n{}",
            truncated,
            random_choice(&reply_starters)
        )
    }

    pub fn branch_name() -> String {
        let prefixes = ["feature", "fix", "refactor", "chore"];
        format!("{}/{}", random_choice(&prefixes), short_uuid())
    }

    /// Generate a legible random username like "john_smith_42"
    pub fn random_username(index: usize) -> String {
        let first: String = FirstName().fake();
        let last: String = LastName().fake();
        // Clean up names to be valid usernames (lowercase, alphanumeric, underscores)
        let first_clean: String = first
            .chars()
            .filter(|c| c.is_alphanumeric())
            .collect::<String>()
            .to_lowercase();
        let last_clean: String = last
            .chars()
            .filter(|c| c.is_alphanumeric())
            .collect::<String>()
            .to_lowercase();
        format!("{}_{}{:03}", first_clean, last_clean, index)
    }

    /// Generate a legible full name like "John Smith"
    pub fn random_full_name() -> String {
        let first: String = FirstName().fake();
        let last: String = LastName().fake();
        format!("{} {}", first, last)
    }

    /// Generate a random email based on name
    pub fn random_email(username: &str) -> String {
        let domains = ["example.com", "test.local", "loadtest.dev"];
        format!("{}@{}", username, random_choice(&domains))
    }
}

fn random_choice<T>(items: &[T]) -> &T {
    let mut rng = rand::thread_rng();
    items.choose(&mut rng).unwrap()
}

fn short_uuid() -> String {
    Uuid::new_v4().to_string()[..12].to_string()
}

fn to_pascal_case(s: &str) -> String {
    s.split(|c: char| !c.is_alphanumeric())
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().chain(chars).collect(),
            }
        })
        .collect()
}

fn random_sentences(count: usize) -> String {
    let mut rng = rand::thread_rng();
    let sentences: Vec<String> = (0..count)
        .map(|_| {
            let word_count = rng.gen_range(8..=15);
            let words: Vec<&str> = TECH_WORDS
                .choose_multiple(&mut rng, word_count)
                .copied()
                .collect();
            let mut sentence = words.join(" ");
            if let Some(first) = sentence.get_mut(0..1) {
                first.make_ascii_uppercase();
            }
            format!("{}.", sentence)
        })
        .collect();
    sentences.join(" ")
}

fn generate_random_methods(class_name: &str) -> String {
    let mut rng = rand::thread_rng();
    let method_count = rng.gen_range(2..=4);
    let suffixes = ["Data", "Request", "Response", "Item", "Entity"];

    let methods: Vec<String> = (0..method_count)
        .map(|i| {
            let suffix = random_choice(&suffixes);
            let method_name = format!("process{}{}", suffix, i);
            format!(
                r#"    public void {}() {{
        // Auto-generated method for {}
        System.out.println("Executing {}");
    }}"#,
                method_name, class_name, method_name
            )
        })
        .collect();

    methods.join("\n\n")
}
