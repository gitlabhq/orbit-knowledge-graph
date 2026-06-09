*** Settings ***
Documentation       Regression suite for stale-data cleanup on re-indexing: after every push to
...                 an already-indexed project, the entire code graph must still be retrievable,
...                 not only the entities from files the push touched. Each assertion goes
...                 through /api/v4/orbit/query with the same File -[DEFINES]-> Definition query
...                 shape the Code Intelligence panel issues, so a cleanup that tombstones
...                 re-emitted rows (or leaves stale rows behind) fails here.

Library             OperatingSystem
Resource            gitlab.resource
Resource            orbit.resource
Resource            git.resource


*** Variables ***
${RUBY_FIXTURE_DIR}     /fixtures/ruby/weather-app
${RUBY_FILE_COUNT}      ${8}
${FORMATTER_PATCH}      \nmodule WeatherApp\nclass Formatter\ndef render_upcase(forecast)\nrender(forecast).upcase\nend\nend\nend\n
${ALERTS_SOURCE}        module WeatherApp\nclass Alerts\ndef active?\nfalse\nend\nend\nend\n


*** Test Cases ***
Project Is Pushed And Fully Indexed
    [Documentation]    Seed the project with the ruby fixture and verify the complete baseline
    ...                graph before any re-index happens.
    [Tags]    code-reindex
    ${suffix}=    Random Suffix
    ${name}=    Set Variable    reindex-ruby-${suffix}
    ${project}=    Create Project    ${name}    ${SHARED_NAMESPACE_ID}
    Push Fixture To Project    ${project}    ${RUBY_FIXTURE_DIR}
    Wait For Node Indexed    Project    ${project["id"]}    ${name}    timeout=180s
    Set Suite Variable    ${PROJECT}    ${project}
    ${work}=    Clone Project Repo    ${project}
    Set Suite Variable    ${WORK}    ${work}
    File Count For Project Is    ${PROJECT}[id]    ${RUBY_FILE_COUNT}
    Code Graph Is Fully Retrievable    ${PROJECT}[id]

Modifying One File Preserves The Rest Of The Graph
    [Documentation]    Push a change to a single file, gate on its new definition appearing
    ...                (proof the re-index ran), then re-verify the full graph: entities from
    ...                untouched files must survive the stale-data cleanup of the new run.
    [Tags]    code-reindex
    Append To File    ${WORK}/lib/weather_app/formatter.rb    ${FORMATTER_PATCH}
    Commit And Push All    ${WORK}    fixture: add Formatter render_upcase
    Definition Exists In Project    ${PROJECT}[id]    WeatherApp::Formatter::render_upcase    Method
    File Count For Project Is    ${PROJECT}[id]    ${RUBY_FILE_COUNT}
    Code Graph Is Fully Retrievable    ${PROJECT}[id]

Adding A File Preserves The Rest Of The Graph
    [Documentation]    Second consecutive re-index: a cleanup that flips entity liveness per run
    ...                passes one re-index and fails the next, so this round catches both
    ...                parities. The new file must land with its panel-shaped DEFINES edge.
    [Tags]    code-reindex
    Create File    ${WORK}/lib/weather_app/alerts.rb    ${ALERTS_SOURCE}
    Commit And Push All    ${WORK}    fixture: add Alerts
    ${alerts}=    Definition Exists In Project    ${PROJECT}[id]    WeatherApp::Alerts    Class
    ${alerts_id}=    Convert To Integer    ${alerts["id"]}
    Set Suite Variable    ${ALERTS_ID}    ${alerts_id}
    File Count For Project Is    ${PROJECT}[id]    ${9}
    Defines Edge Exists Between File And Definition    ${PROJECT}[id]
    ...    lib/weather_app/alerts.rb    WeatherApp::Alerts
    Definition Exists In Project    ${PROJECT}[id]    WeatherApp::Formatter::render_upcase    Method
    Code Graph Is Fully Retrievable    ${PROJECT}[id]

Deleting A File Removes Its Entities And Preserves The Rest
    [Documentation]    Stale cleanup must still do its real job: entities of a deleted file
    ...                disappear from query results while everything else stays retrievable.
    [Tags]    code-reindex
    Remove File    ${WORK}/lib/weather_app/alerts.rb
    Commit And Push All    ${WORK}    fixture: remove Alerts
    Wait For Node Removed    Definition    ${ALERTS_ID}    timeout=240s
    File Count For Project Is    ${PROJECT}[id]    ${RUBY_FILE_COUNT}
    Definition Exists In Project    ${PROJECT}[id]    WeatherApp::Formatter::render_upcase    Method
    Code Graph Is Fully Retrievable    ${PROJECT}[id]


*** Keywords ***
Code Graph Is Fully Retrievable
    [Documentation]    Assert the canonical entities of every original fixture file are
    ...                queryable: definitions by fqn, plus the File -[DEFINES]-> Definition
    ...                traversal the Code Intelligence panel uses for each source file.
    [Arguments]    ${project_id}
    Definition Exists In Project    ${project_id}    WeatherApp    Module
    Definition Exists In Project    ${project_id}    WeatherApp::Forecast    Class
    Definition Exists In Project    ${project_id}    WeatherApp::CLI    Class
    Definition Exists In Project    ${project_id}    WeatherApp::Formatter    Class
    Definition Exists In Project    ${project_id}    WeatherApp::WeatherService    Class
    Definition Exists In Project    ${project_id}    WeatherApp::UnknownCityError    Class
    Definition Exists In Project    ${project_id}    WeatherApp::Forecast::temperature_f    Method
    Definition Exists In Project    ${project_id}    WeatherApp::CLI::run    Method
    Defines Edge Exists Between File And Definition    ${project_id}
    ...    lib/weather_app.rb    WeatherApp
    Defines Edge Exists Between File And Definition    ${project_id}
    ...    lib/weather_app/forecast.rb    WeatherApp::Forecast
    Defines Edge Exists Between File And Definition    ${project_id}
    ...    lib/weather_app/cli.rb    WeatherApp::CLI
    Defines Edge Exists Between File And Definition    ${project_id}
    ...    lib/weather_app/formatter.rb    WeatherApp::Formatter
    Defines Edge Exists Between File And Definition    ${project_id}
    ...    lib/weather_app/weather_service.rb    WeatherApp::WeatherService
    Defines Edge Exists Between Definitions    ${project_id}
    ...    WeatherApp::Forecast    WeatherApp::Forecast::temperature_f
    Defines Edge Exists Between Definitions    ${project_id}
    ...    WeatherApp::CLI    WeatherApp::CLI::run
