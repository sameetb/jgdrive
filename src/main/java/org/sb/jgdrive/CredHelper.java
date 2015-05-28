package org.sb.jgdrive;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.Optional;
import java.util.logging.Logger;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeTokenRequest;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.DriveScopes;

class CredHelper
{
    private static final String userID = "jgdrive";
    private static final Logger log = Logger.getLogger(CredHelper.class.getPackage().getName());
    private static final String redirURI = "urn:ietf:wg:oauth:2.0:oob";
    private final Path userToken;
    private final HttpTransport httpTransport;
    private final JsonFactory jfac;
    private final GoogleClientSecrets clientSecrets;
    
    CredHelper(Path jgdrive, HttpTransport httpTransport, JsonFactory jfac) throws IOException
    {
        if(Files.notExists(jgdrive)) throw new FileNotFoundException("The path "  + jgdrive + " does not exist.");
        this.httpTransport = httpTransport;
        this.jfac = jfac;
        userToken = jgdrive.resolve("usertoken");
        Path clientSecretsFile = jgdrive.resolve("client_secrets.json");
        while(Files.notExists(clientSecretsFile))
        {
            log.fine("Could not find " + clientSecretsFile.toAbsolutePath());
            System.out.println("Please type the absolute path of the 'client_secrets.json' file:");
            Path src = Paths.get(readLine());
            if(Files.exists(src) && !Files.isDirectory(src))
            {
                log.info("Copying " + src + " to " + clientSecretsFile);
                Files.copy(src, clientSecretsFile, StandardCopyOption.REPLACE_EXISTING);
            }
            else
                log.info("Path " + src + " is not valid.");
        }
        clientSecrets = GoogleClientSecrets.load(jfac, new FileReader(clientSecretsFile.toFile()));
    }

    Credential authorize() throws GeneralSecurityException, IOException
    {
        GoogleAuthorizationCodeFlow flow = getFlow(true);
        System.out.println("Please open the following URL in your browser, login, accept and then type the authorization code shown on the page:");
        System.out.println(" " + flow.newAuthorizationUrl().setRedirectUri(redirURI).build());
        String code = readLine();
        GoogleTokenResponse response =
                new GoogleAuthorizationCodeTokenRequest(httpTransport, jfac,
                        clientSecrets.getDetails().getClientId(), clientSecrets.getDetails().getClientSecret(),
                        code, redirURI).execute();
        log.fine(() -> "Got token response:" + response);
        Credential cred = flow.createAndStoreCredential(response, userID);
        return cred;
    }

    private GoogleAuthorizationCodeFlow getFlow(boolean force) throws IOException
    {
        return new GoogleAuthorizationCodeFlow.Builder(httpTransport, jfac, 
                                                                clientSecrets, Collections.singleton(DriveScopes.DRIVE))
                        .setAccessType("offline")
                        .setApprovalPrompt(force ? "force" : "auto")
                        .setDataStoreFactory(new FileDataStoreFactory(userToken.toFile()))
                        .build();
    }
    
    Optional<Credential> get() throws IOException
    {
        GoogleAuthorizationCodeFlow flow = getFlow(false);
        return Optional.ofNullable(flow.loadCredential(userID));
    }

    private static String readLine() throws IOException
    {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String code = br.readLine();
        br.close();
        return code != null ? code.trim() : "";
    }
}
