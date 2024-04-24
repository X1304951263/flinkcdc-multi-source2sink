package org.example.sink;


import java.io.Serializable;

public class JdbcConnectionOptions implements Serializable {

    private static final long serialVersionUID = 1234234234234L;

    private String url;
    private String user;
    private String password;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String url;
        private String user;
        private String password;

        public Builder url(String url) {
            this.url = url;
            return this;
        }
        public Builder user(String user) {
            this.user = user;
            return this;
        }
        public Builder password(String password) {
            this.password = password;
            return this;
        }
        public JdbcConnectionOptions build() {
            JdbcConnectionOptions options = new JdbcConnectionOptions();
            options.setUrl(url);
            options.setUser(user);
            options.setPassword(password);
            return options;
        }

    }
}
