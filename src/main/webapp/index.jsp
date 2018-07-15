
<html>
  <head>
    <meta http-equiv="refresh" content="10">
  </head>
  <title>test Yi Zhao</title>
  <body>
    <h3> test Yi Zhao </h3>
    <%@page import="com.example.appengine.pubsub.PubSubPublish "%>
    <%
      PubSubPublish pubSubPublish = new PubSubPublish();
      pubSubPublish.runPub();
    %>
  </body>
</html>
