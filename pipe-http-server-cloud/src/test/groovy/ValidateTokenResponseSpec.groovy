import com.tesco.aqueduct.pipe.identity.validator.ValidateTokenResponse
import spock.lang.Specification
import spock.lang.Unroll

class ValidateTokenResponseSpec extends Specification {

    static final def USER_ID = "userId"
    static final def INVALID = "INVALID"
    static final def VALID = "VALID"
    static final def VALID_CLAIMS = [
            new ValidateTokenResponse.Claim(ValidateTokenResponse.Claim.CONFIDENCE_LEVEL_CLAIM, "12", null),
            new ValidateTokenResponse.Claim(ValidateTokenResponse.Claim.FORMER_USER_KEY_CLAIM, null, Arrays.asList(new ValidateTokenResponse.Values("trn:tesco:uid:uuid:00062028-a4f5-4620-a48d-5ae252bd7070"))),
            new ValidateTokenResponse.Claim(ValidateTokenResponse.Claim.MERGED_CLAIM, null, Arrays.asList(new ValidateTokenResponse.MultiKeyValues("trn:tesco:uid:uuid:00062028-a4f5-4620-a48d-5ae252bd7070", "OnlineUuid"))),
    ]

    @Unroll
    def "Verify token is #description"() {
        given:
        def response = new ValidateTokenResponse(USER_ID, status, claims)

        expect:
        expected == response.isTokenValid()

        where:
        status  | claims         || expected | description
        INVALID | []             || false    | "invalid when status is invalid and no claims are present"
        INVALID | VALID_CLAIMS   || false    | "invalid when status is invalid but claims are valid"
        VALID   | VALID_CLAIMS   || true     | "valid when both status and claims are valid"
    }
}