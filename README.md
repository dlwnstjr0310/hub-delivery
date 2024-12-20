# 🚚 **배송 관리 서비스**
![image](https://github.com/user-attachments/assets/37e0de1e-9741-448f-92e8-7bd526ca4b47)

## **프로젝트 개요**
사용자에게 **배송 요청 생성, 상태 관리, 추적 및 조회**하는 기능을 제공합니다.  


---

## 🛠️ **사용 기술 스택**
- **Backend**: `Java`,`Kotlin` `Spring Boot`, `JPA (Hibernate)`
- **Database**: `MySQL`, 
- **Cache**: `Redis`
- **Deployment**: `Docker`
- **Authentication**: `JWT`
- **Build**: `Gradle`

---
## 👥 **팀원별 역할**  

| 이름         | 역할 및 기여 내용                                      |
|--------------------------|------------------------------------------------------|
| **김성호 (Leader)**      | - slack 메시지 관리<br> - product 관리   |
| **이준석**  | - hub route 최적화 <br>- company 관리 <br>- Gateway               |
| **고혜정** | - order 관리<br>- delivery 관리     |
| **강희진**        | - 유저 관리 <br>- auth |

---
## 📋 **요구 사항**

### 📌 **요약**

1. **허브 관리**: 허브 정보를 생성하고, 조회 및 수정하며 soft delete로 관리한다.  
2. **업체 관리**: 생산업체와 수령업체를 등록하고, 각 업체는 특정 허브에 소속된다.  
3. **상품 관리**: 업체별 상품 정보를 생성 및 수정하고 허브에 소속시킨다.  
4. **주문 생성**: 수령업체가 상품을 주문하면 재고를 확인하고 주문 정보를 저장한다.  
5. **배송 경로 생성**: 주문 시 허브 간 최적 경로를 설정하고 배송 정보를 생성한다.  
6. **배송 담당자 배정**: 허브 또는 업체 배송 담당자를 순차적으로 배정한다.  
7. **배송 상태 관리**: 배송 상태(대기, 이동 중, 도착)를 실시간으로 업데이트한다.  
8. **Slack 알림**: 발송 허브 담당자에게 최종 발송 시한 및 주문 상태를 슬랙으로 알림. 

---

## 📊 **ERD**

아래는 배송 관리 서비스의 **ERD 설계**입니다:
<img src="https://github.com/user-attachments/assets/697acd81-44e4-4f1b-9938-eff31b06249d" alt="delivery_service_erd" style="border: 1px solid #ddd; border-radius: 5px; ">


---

## 🏗️ **인프라 설계**

서비스의 인프라는 다음과 같이 구성되어 있습니다:  
<img src="https://github.com/user-attachments/assets/482578dd-c752-49be-aa65-0ab3655e2e78" alt="delivery_service_infra" style="border: 1px solid #ddd; border-radius: 5px; width: 500px;">
![kafka drawio (1)](https://github.com/user-attachments/assets/d7c4da03-8365-4ea7-9edf-06a3558cd3d2)

---

## 🚀 **주요 기능**

### **hub 서비스**

등록된 허브 정보를 바탕으로 허브가 인접한 다른 허브와 직접 연결되어 있어, 특정 경로를 통해 릴레이처럼 전달하는 구조로 구현. <br>
다익스트라 알고리즘을 활용하여 최단 시간을 기준으로 경로 탐색

## **delivery route 서비스**

주문 이벤트 수신 후 주문 정보에서 출발점과 도착지점 간의 최단 거리를 계산하여 해당 주문의 경로를 저장하는 로직 구현

### **order 서비스**

사용자가 주문을 생성하고 관리할 수 있도록 하는 주문 생성, 수정, 조회 기능을 개발</br>
주문 생성 이벤트를 Kafka를 통해 다른 서비스와 비동기로 통신하도록 구현

### **delivery 서비스**

주문 데이터를 기반으로 배송 정보를 처리하는 배송 생성, 수정, 삭제, 조회 기능을 구현</br>
Kafka Consumer를 통해 order 서비스에서 발생한 이벤트를 실시간으로 수신하고 배송 상태를 업데이트하는 로직을 추가, 배송 생성 이벤트를 Kafka를 통해 다른 서비스와 비동기로 통신하도록 구현


### **product 서비스**

Kafka Consumer를 통해 order 서비스에서 발행된 이벤트를 수신하고 재고를 감소시킴.

### **slack 서비스**

Kafak Consumer를 통해 delivery 서비스를 수신하고 hub, company, order, user 등에 feignclient를 통해 메세지를 generate하여 slack으로 발행


### **user 서비스**
인증/인가와 사용자의 생성, 수정, 삭제, 조회 기능을 제공하며 회원 관리를 담당.</br>
중요 API에는 AOP 기반의 사용자 상태 검증을 적용해 삭제된 사용자나 비활성화된 사용자의 접근을 차단.

### **deliverypersons 서비스**
배송 담당자를 관리하는 서비스로, 배송 담당자의 생성, 수정, 삭제, 조회 기능을 제공.</br>

---

## 🛠️ **트러블 슈팅**

### **1. 카프카 설정 문제 (혜정)**
- **문제**: Kafka Consumer를 설정하던 중 JsonDeserializer가 역직렬화할 대상 클래스를 찾지 못하면서 ClassNotFoundException이 발생했습니다.
- **해결**: Kafka 설정 파일인 KafkaConsumerConfig.java에 기타 다른 설정들을 아예 지운뒤 해당 코드만 남기고 yml 을 통해 최소 설정만 했더니 오히려 발생했던 에러가 사라졌습니다.

```java
@Configuration
@EnableKafka
public class KafkaConsumerConfig {
  @Bean
  public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
    return new KafkaTemplate<>(producerFactory);
  }
}
```


### **2. Swagger와 ControllerAdvice 어노테이션 충돌 현상 (성호)**
- **문제**: controllerAdvice랑 swagger가 충돌하여 swagger가 제대로 작동하지 않았습니다.
- **해결**: 
1. Swagger가 Controller 관련 Bean을 스캔할 때, @ControllerAdvice가 붙은 클래스도 컨트롤러의 일부로 오해하고 스캔하는 현상이 있었습니다.
2. @ExceptionHandler가 정의된 메서드까지 API 문서에 포함되게 되면서 불필요한 문서가 Swagger에 나타나는 겹치는 이유였습니다.
3. GlobalHandlerException에 @Hidden 어노테이션을 달아서 swagger가 스캔하지 않도록 명시할 수 있었습니다.

```java
@RestControllerAdvice
@Hidden
public class GlobalExceptionHandler {


    @ExceptionHandler(ProductNullPointerException.class)
    public CustomApiResponse<ErrorResponse> handleNullPointerException(ProductNullPointerException ex) {
        return buildErrorResponse(Error.NOT_FOUND_PRODUCT);
    }


    private CustomApiResponse<ErrorResponse> buildErrorResponse(Error error) {
        return new CustomApiResponse<>(error.getCode(), error.getMessage());
    }
}
```


### **3. 회원 삭제 시 접근 제한 문제 (희진)**
- **문제**: 회원이 삭제되었더라도 이미 발급된 JWT 토큰이 유효한 상태라면 서버에 접근할 수 있는 문제가 발생했습니다.
- **해결**: 
1. 레디스 기반 블랙리스트를 사용해 토큰을 무효화하려 했으나, 이미 발급된 토큰을 서버가 알 수 없어 실시간 차단이 어려웠습니다.
2. AOP 기반의 횡단 관심사 처리를 통해 공통 검증 로직을 구현했습니다.
3. @CheckUserStatus 어노테이션을 생성하여 중요 API에만 사용자 상태 검증을 적용.
4. AOP를 통해 API 진입 시점에서 사용자 상태를 검증하도록 구현했습니다.

```java
@GetMapping("/{id}")
@CheckUserStatus
public Response<UserResponse> getUserById(@PathVariable UUID id) {

    UserResponse user = userService.getUserById(id);

    return new Response<>(
            HttpStatus.OK.value(),
            HttpStatus.OK.getReasonPhrase(),
            user
    );
}
```


---


## 🧩 **개선점**
- slack 같은 경우 여러 도메인으로 feignClient를 보내야 할 상황이 있었는데, competableFuture 객체를 활용하여 비동기적으로 수행하면 효율이 더 좋을 것 같습니다.
- ElasticSearch를 통한 Shard와 Replica로 데이터를 분산 처리하며 배송 이력 조회 성능을 최적화할 수 있을 것으로 보입니다.

---

## 🎤 **소감**

### **혜정**  

> Test를 세분화해 미리 진행하지 않고 마지막에 몰아서 한 점이 부족하다 느껴졌고 또 다른 조원 분들에 비해 msa 나 docker 를 접해보지 못해 다루는데에 미숙했던 점이 보안되야할 점이라고 느껴집니다. </br>
> 짧은 시간동안 너무나 부족한 점을 많이 느끼고 또 공부의 필요성을 느꼈던 2주 였습니다. </br>
> 그동안 접해보지 못했던 새로운 기술들과 구조를 배울 수 있던 좋은 시간이었던거 같습니다!
> 다들 많이 도와주시고 알려주셔서 감사합니다!

### **성호**  

> 저도 마찬가지로, MSA에 미숙하여 설정 파일을 구축하는 것에 어려움을 느꼈습니다.</br>
> 처음에 도커라던지, 파일들의 컨벤션을 정하는 것의 중요성을 몸소 실감하였습니다</br>
> 팀원들이 열심히 잘 해주셔서 감사했고, 자극도 많이 받아가는 시간이었습니다!

### **준석**

> 시스템의 흐름을 머리속으로 그려보면서 그에 맞는 로직을 생각해보면서 참 재미있다고 느꼈습니다.
> 다만, 재미만 있었지 프로젝트를 진행하면서 생각했던 흐름이 엉망이었다는걸 느끼게 되었고,
> 플로우차트나 시퀀스 다이어그램등을 조금 더 활용할 수 있으면 좋겠다는 생각이 참 많이 들었습니다.
> 혼자 생각했던 엉망인 흐름때문에 팀원들에게 민폐를 끼친것같아 죄송하네요. 끝까지 짜증내지않고 들어주셔서 감사했습니다.

### **희진**
> 멋진 팀을 만나 많이 성장할 수 있었습니다. msa, kafka 등 처음 접해보는 개념이 많아서 개발을 하는데,
> 많은 시간 지체가 있었습니다. 결국 시간 조절 문제로 제가 맡은 부분을 완성도 있게 끝내지 못하여 많은 아쉬움이 남습니다.
> 그럼에도 팀이 있었기에 여기까지 할 수 있었던 거 같습니다. 감사합니다.

