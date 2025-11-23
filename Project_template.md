# Задание 1

[C4._Component.pulm](src/diagrams/C4_Container.puml)

# Задание 2

### 1. Proxy

- Запросы к API Gateway:
```bash
curl http://localhost:8000/api/movies
[{"id":1,"title":"The Shawshank Redemption","description":"Two imprisoned men bond over a number of years, finding solace and eventual redemption through acts of common decency.","genres":["Drama"],"rating":9.3},{"id":2,"title":"The Godfather","description":"The aging patriarch of an organized crime dynasty transfers control of his clandestine empire to his reluctant son.","genres":["Crime","Drama"],"rating":9.2},{"id":3,"title":"The Dark Knight","description":"When the menace known as the Joker wreaks havoc and chaos on the people of Gotham, Batman must accept one of the greatest psychological and physical tests of his ability to fight injustice.","genres":["Action","Crime","Drama"],"rating":9},{"id":4,"title":"Pulp Fiction","description":"The lives of two mob hitmen, a boxer, a gangster and his wife, and a pair of diner bandits intertwine in four tales of violence and redemption.","genres":["Crime","Drama"],"rating":8.9},{"id":5,"title":"Forrest Gump","description":"The presidencies of Kennedy and Johnson, the Vietnam War, the Watergate scandal and other historical events unfold from the perspective of an Alabama man with an IQ of 75, whose only desire is to be reunited with his childhood sweetheart.","genres":["Drama","Romance"],"rating":8.8}]
```

### 2. Kafka
Локально порт 8090 занят другим проектом, потому кафка запущен на http://localhost:8091 

[npm_test_result.jpg](src/screenshots/npm_test_result.jpg)
[kafka_board.jpeg](src/screenshots/kafka_board.jpeg)
[kafka_topics.jpg](src/screenshots/kafka_topics.jpg)
[event_service_logs.jpg](src/screenshots/event_service_logs.jpg)

# Задание 3

### CI/CD

[git_workflow.jpg](src/screenshots/git_workflow.jpg)

### Proxy в Kubernetes

[cinemaabyss_api_result.jpg](src/screenshots/cinemaabyss_api_result.jpg)
[even_service_logs_2.jpg](src/screenshots/event_service_logs_2.jpg)

# Задание 4

[cinemaabyss_api_result_last.jpg](src/screenshots/cinemaabyss_api_result_last.jpg)
[helm_install.jpg](src/screenshots/helm_install.jpg)
[kubeclt_pods.jpg](src/screenshots/kubectl_pods.jpg)

