*** Settings ***
Documentation       Push fixture code into ruby- and java-named projects under the shared
...                 namespace and verify the code-graph (files, definitions, structural
...                 DEFINES + IMPORTS edges) lands in the property graph via the Orbit
...                 query API. Every assertion goes through /api/v4/orbit/query — the
...                 system under test.

Resource            gitlab.resource
Resource            orbit.resource
Resource            git.resource


*** Variables ***
${RUBY_FIXTURE_DIR}     /fixtures/ruby/weather-app
${JAVA_FIXTURE_DIR}     /fixtures/java/weather-app

# Expected fixture cardinalities (must match e2e/fixtures/*/weather-app/).
${RUBY_FILE_COUNT}      ${8}
${JAVA_FILE_COUNT}      ${8}


*** Test Cases ***
Ruby Weather Project Is Pushed And Indexed
    [Documentation]    Create ruby-weather-<random>, push the ruby fixture, wait until the
    ...                project node appears via Orbit. Stores ${RUBY_PROJECT} for follow-up cases.
    [Tags]    code-indexing
    ${suffix}=    Random Suffix
    ${name}=    Set Variable    ruby-weather-${suffix}
    ${project}=    Create Project    ${name}    ${SHARED_NAMESPACE_ID}
    Push Fixture To Project    ${project}    ${RUBY_FIXTURE_DIR}
    Wait For Node Indexed    Project    ${project["id"]}    ${name}    timeout=180s
    Set Suite Variable    ${RUBY_PROJECT}    ${project}

Java Weather Project Is Pushed And Indexed
    [Documentation]    Create java-weather-<random>, push the java fixture, wait until the
    ...                project node appears via Orbit. Stores ${JAVA_PROJECT} for follow-up cases.
    [Tags]    code-indexing
    ${suffix}=    Random Suffix
    ${name}=    Set Variable    java-weather-${suffix}
    ${project}=    Create Project    ${name}    ${SHARED_NAMESPACE_ID}
    Push Fixture To Project    ${project}    ${JAVA_FIXTURE_DIR}
    Wait For Node Indexed    Project    ${project["id"]}    ${name}    timeout=180s
    Set Suite Variable    ${JAVA_PROJECT}    ${project}

Ruby Project Code Graph Has Expected Files And Definitions
    [Documentation]    Assert exact File count and presence of canonical Ruby definitions
    ...                (Module, Classes, instance + singleton methods).
    [Tags]    code-indexing    code-graph
    ${pid}=    Set Variable    ${RUBY_PROJECT}[id]
    File Count For Project Is    ${pid}    ${RUBY_FILE_COUNT}
    Definition Count For Project Is At Least    ${pid}    ${15}
    Definition Exists In Project    ${pid}    WeatherApp                              Module
    Definition Exists In Project    ${pid}    WeatherApp::Forecast                    Class
    Definition Exists In Project    ${pid}    WeatherApp::CLI                         Class
    Definition Exists In Project    ${pid}    WeatherApp::Formatter                   Class
    Definition Exists In Project    ${pid}    WeatherApp::WeatherService              Class
    Definition Exists In Project    ${pid}    WeatherApp::UnknownCityError            Class
    Definition Exists In Project    ${pid}    WeatherApp::Forecast::temperature_f     Method
    Definition Exists In Project    ${pid}    WeatherApp::CLI::run                    Method
    Definition Exists In Project    ${pid}    WeatherApp::CLI::run                    SingletonMethod

Ruby Project Code Graph Has Expected Containment Edges
    [Documentation]    Assert structural DEFINES edges (Module→Class, Class→Method,
    ...                File→top-level Definition) for the ruby fixture.
    [Tags]    code-indexing    code-graph
    ${pid}=    Set Variable    ${RUBY_PROJECT}[id]
    Defines Edge Exists Between Definitions    ${pid}
    ...    WeatherApp                                  WeatherApp::Forecast
    Defines Edge Exists Between Definitions    ${pid}
    ...    WeatherApp                                  WeatherApp::CLI
    Defines Edge Exists Between Definitions    ${pid}
    ...    WeatherApp::Forecast                        WeatherApp::Forecast::temperature_f
    Defines Edge Exists Between Definitions    ${pid}
    ...    WeatherApp::CLI                             WeatherApp::CLI::run
    Defines Edge Exists Between File And Definition    ${pid}
    ...    lib/weather_app.rb                          WeatherApp
    Defines Edge Exists Between File And Definition    ${pid}
    ...    lib/weather_app/forecast.rb                 WeatherApp::Forecast

Java Project Code Graph Has Expected Files And Definitions
    [Documentation]    Assert exact File count and presence of canonical Java definitions
    ...                (Classes spread across cli/model/service/formatter packages, plus
    ...                an Enum and a Record).
    [Tags]    code-indexing    code-graph
    ${pid}=    Set Variable    ${JAVA_PROJECT}[id]
    File Count For Project Is    ${pid}    ${JAVA_FILE_COUNT}
    Definition Count For Project Is At Least    ${pid}    ${20}
    Definition Exists In Project    ${pid}    com.example.weather.Main                                Class
    Definition Exists In Project    ${pid}    com.example.weather.Main.run                            Method
    Definition Exists In Project    ${pid}    com.example.weather.cli.Cli                             Class
    Definition Exists In Project    ${pid}    com.example.weather.cli.Cli.parse                       Method
    Definition Exists In Project    ${pid}    com.example.weather.service.WeatherService              Class
    Definition Exists In Project    ${pid}    com.example.weather.service.WeatherService.fetch        Method
    Definition Exists In Project    ${pid}    com.example.weather.service.UnknownCityException        Class
    Definition Exists In Project    ${pid}    com.example.weather.formatter.Formatter                 Class
    Definition Exists In Project    ${pid}    com.example.weather.formatter.Formatter.Format          Enum
    Definition Exists In Project    ${pid}    com.example.weather.model.Forecast                      Record

Java Project Code Graph Has Expected Containment Edges
    [Documentation]    Assert DEFINES edges (Class→Method, File→top-level Class, Enum→EnumConstant)
    ...                for the java fixture.
    [Tags]    code-indexing    code-graph
    ${pid}=    Set Variable    ${JAVA_PROJECT}[id]
    Defines Edge Exists Between Definitions    ${pid}
    ...    com.example.weather.Main                            com.example.weather.Main.run
    Defines Edge Exists Between Definitions    ${pid}
    ...    com.example.weather.cli.Cli                         com.example.weather.cli.Cli.parse
    Defines Edge Exists Between Definitions    ${pid}
    ...    com.example.weather.service.WeatherService          com.example.weather.service.WeatherService.fetch
    Defines Edge Exists Between Definitions    ${pid}
    ...    com.example.weather.formatter.Formatter.Format      com.example.weather.formatter.Formatter.Format.JSON
    Defines Edge Exists Between File And Definition    ${pid}
    ...    src/main/java/com/example/weather/Main.java         com.example.weather.Main
    Defines Edge Exists Between File And Definition    ${pid}
    ...    src/main/java/com/example/weather/model/Forecast.java    com.example.weather.model.Forecast

Java Project Has Expected Imports
    [Documentation]    Assert IMPORTS edges from project Files into ImportedSymbol nodes
    ...                originating from java.* packages and from intra-project packages.
    [Tags]    code-indexing    code-graph
    ${pid}=    Set Variable    ${JAVA_PROJECT}[id]
    Imports Edge Exists Between File And Imported Symbol    ${pid}
    ...    src/main/java/com/example/weather/cli/Cli.java
    ...    java.io                 PrintStream
    Imports Edge Exists Between File And Imported Symbol    ${pid}
    ...    src/main/java/com/example/weather/cli/Cli.java
    ...    java.util               Arrays
    Imports Edge Exists Between File And Imported Symbol    ${pid}
    ...    src/main/java/com/example/weather/service/WeatherService.java
    ...    java.util.stream        Collectors
    Imports Edge Exists Between File And Imported Symbol    ${pid}
    ...    src/main/java/com/example/weather/Main.java
    ...    com.example.weather.model    Forecast
