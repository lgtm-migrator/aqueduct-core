package Helper

import com.stehno.ersatz.Decoders
import com.stehno.ersatz.ErsatzServer
import groovy.json.JsonOutput

class IdentityMock {

    private ErsatzServer ersatzServer
    private String clientId
    private String clientSecret
    private String clientIdAndSecret
    private String accessToken

    IdentityMock(String clientId, String clientSecret, String accessToken) {
        this.accessToken = accessToken
        this.clientSecret = clientSecret
        this.clientId = clientId
        this.clientIdAndSecret = "trn:tesco:cid:$clientId:$clientSecret"
        this.ersatzServer = new ErsatzServer({
            decoder('application/json', Decoders.utf8String)
            reportToConsole()
        })
        ersatzServer.start()
    }

    void clearExpectations() {
        ersatzServer.clearExpectations()
    }

    String getUrl() {
        ersatzServer.getHttpUrl()
    }

    void issueValidTokenFromIdentity() {
        def requestJson = JsonOutput.toJson([
                client_id       : clientId,
                client_secret   : clientSecret,
                grant_type      : "client_credentials",
                scope           : "internal public",
                confidence_level: 12
        ])

        ersatzServer.expectations {
            post("/v4/issue-token/token") {
                body(requestJson, "application/json")
                header("Accept", "application/vnd.tesco.identity.tokenresponse+json")
                header("Content-Type", "application/json")
                called(1)

                responder {
                    header("Content-Type", "application/vnd.tesco.identity.tokenresponse+json")
                    body("""
                    {
                        "access_token": "${accessToken}",
                        "token_type"  : "bearer",
                        "expires_in"  : 1000,
                        "scope"       : "some: scope: value"
                    }
                    """)
                }
            }
        }
    }

    void acceptIdentityTokenValidationRequest() {
        def json = JsonOutput.toJson([access_token: accessToken])

        ersatzServer.expectations {
            post('/v4/access-token/auth/validate') {
                queries("client_id": "$clientIdAndSecret")
                body(json, "application/json")
                called(1)

                responder {
                    header("Content-Type", "application/json;charset=UTF-8")
                    body("""
                        {
                          "UserId": "someClientUserId",
                          "Status": "VALID",
                          "Claims": [
                            {
                              "claimType": "http://schemas.tesco.com/ws/2011/12/identity/claims/clientid",
                              "value": "trn:tesco:cid:${UUID.randomUUID()}"
                            },
                            {
                              "claimType": "http://schemas.tesco.com/ws/2011/12/identity/claims/scope",
                              "value": "oob"
                            },
                            {
                              "claimType": "http://schemas.tesco.com/ws/2011/12/identity/claims/userkey",
                              "value": "trn:tesco:uid:uuid:${UUID.randomUUID()}"
                            },
                            {
                              "claimType": "http://schemas.tesco.com/ws/2011/12/identity/claims/confidencelevel",
                              "value": "12"
                            },
                            {
                              "claimType": "http://schemas.microsoft.com/ws/2008/06/identity/claims/expiration",
                              "value": "1548413702"
                            }
                          ]
                        }
                    """)
                }
            }
        }
    }
}
