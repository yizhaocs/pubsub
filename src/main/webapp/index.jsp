
<html>
  <head>
    <meta http-equiv="refresh" content="10">
  </head>
  <title>test Yi Zhao</title>
  <body>
    <h3> test Yi Zhao </h3>
    <%@page import="com.adara.hackathon.pub.PubSubPublish "%>
    <%@ page import="com.adara.hackathon.sub.Sub" %>
    <%
     PubSubPublish pubSubPublish = new PubSubPublish();
      pubSubPublish.runPub();
      Sub sub = new Sub();
      sub.init();
    %>
  </body>
</html>
