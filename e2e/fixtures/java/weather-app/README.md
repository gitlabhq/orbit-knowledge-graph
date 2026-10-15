# weather-app (Java)

E2e fixture for the code indexing suites. Suites 03 and 04 push this repository to GitLab, and Orbit indexes it.
The app uses mocked data and needs no network. See [E2E testing harness](../../../../docs/dev/e2e-testing.md).

```shell
mvn -B package
java -jar target/weather-app.jar --city Berlin
```
