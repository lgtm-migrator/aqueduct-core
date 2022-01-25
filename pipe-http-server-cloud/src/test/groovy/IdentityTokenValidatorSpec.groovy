import com.tesco.aqueduct.pipe.identity.issuer.IdentityServiceUnavailableException
import com.tesco.aqueduct.pipe.identity.validator.*
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.security.authentication.Authentication
import io.reactivex.Flowable
import io.reactivex.exceptions.CompositeException
import spock.lang.Specification
import spock.lang.Unroll

class IdentityTokenValidatorSpec extends Specification {

    private static final ValidateTokenResponse VALID_RESPONSE = new ValidateTokenResponse(ValidateTokenResponseSpec.USER_ID, ValidateTokenResponseSpec.VALID, ValidateTokenResponseSpec.VALID_CLAIMS)
    private static final ValidateTokenResponse INVALID_RESPONSE = new ValidateTokenResponse(ValidateTokenResponseSpec.USER_ID, ValidateTokenResponseSpec.INVALID, [])
    private static final String clientUid = ValidateTokenResponseSpec.USER_ID

    @Unroll
    def "Validate #description"() {
        given:
        TokenUser tokenUser = new TokenUser("name")
        def clientId = "someClientId"
        def clientSecret = "someClientSecret"

        tokenUser.clientId = clientUid

        and: "token validator"
        def identityTokenValidatorClient = Mock(IdentityTokenValidatorClient) {
            validateToken(_ as String, _ as ValidateTokenRequest, clientId + ":" + clientSecret) >> Flowable.just(identityResponse)
        }
        def tokenValidator = new IdentityTokenValidator(identityTokenValidatorClient, clientId, clientSecret, [tokenUser])

        when:
        def result = tokenValidator.validateToken("token", Mock(HttpRequest)) as Flowable

        then:
        predicate(result)

        where:
        identityResponse || predicate                                                                                     | description
        INVALID_RESPONSE || { Flowable r -> r.isEmpty().blockingGet() }                                                   | 'no authorisation if invalid identity response'
        VALID_RESPONSE   || { Flowable<Authentication> r -> r.blockingFirst().name == ValidateTokenResponseSpec.USER_ID } | 'authorised if valid identity response'
    }

    def "Http client response error is propagated when identity client returns 4xx status code"() {
        given: "token validator"
        def identityTokenValidatorClient = Mock(IdentityTokenValidatorClient) {
            validateToken(_ as String, _ as ValidateTokenRequest, _ as String) >> Flowable.error(new HttpClientResponseException("4xx", HttpResponse.badRequest()))
        }
        def tokenValidator = new IdentityTokenValidator(identityTokenValidatorClient, "clientId", "clientSecret", ["tokenUser"] as List<TokenUser>)

        when:
        (tokenValidator.validateToken("token", Mock(HttpRequest)) as Flowable).blockingSubscribe()

        then:
        thrown(HttpClientResponseException)
    }

    def "Http client response error is propagated when identity client returns 5xx status code"() {
        given: "token validator"
        def identityTokenValidatorClient = Mock(IdentityTokenValidatorClient) {
            validateToken(_ as String, _ as ValidateTokenRequest, _ as String) >> Flowable.error(new HttpClientResponseException("5xx", HttpResponse.serverError()))
        }
        def tokenValidator = new IdentityTokenValidator(identityTokenValidatorClient, "clientId", "clientSecret", ["tokenUser"] as List<TokenUser>)

        when:
        (tokenValidator.validateToken("token", Mock(HttpRequest)) as Flowable).blockingSubscribe()

        then:
        CompositeException exception = thrown()
        exception.getExceptions().last().class == IdentityServiceUnavailableException
    }
}
