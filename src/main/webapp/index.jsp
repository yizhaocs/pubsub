
<html>
  <head>
    <meta http-equiv="refresh" content="10">
  </head>
  <title>test Yi Zhao</title>
  <body>
    <h3> test Yi Zhao </h3>
    <%@page import="com.adara.hackathon.pub.PubMain "%>
    <%@ page import="com.adara.hackathon.sub.SubMain" %>
    <%PubMain pubSubPublish = new PubMain();
      pubSubPublish.runPub();
      SubMain sub = new SubMain();
      sub.init();
    %>
  </body>
</html>
