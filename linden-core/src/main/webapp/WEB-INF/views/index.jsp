<!DOCTYPE html>
<html lang="en" ng-app="linden">
<head>
	<meta charset="utf-8">
	<meta http-equiv="X-UA-Compatible" content="IE=edge">
	<meta name="viewport" content="width=device-width, initial-scale=1">
	<meta name="description" content="">
	<meta name="author" content="">
	<link rel="shortcut icon" href="ico/favicon.ico">
	<title>Linden Admin</title>
	<!-- Bootstrap core CSS -->
	<link href="resources/libs/bootstrap/css/bootstrap.min.css" rel="stylesheet" media="screen">
    <link href="resources/libs/font-awesome/css/font-awesome.min.css" rel="stylesheet" type="text/css">
	<link href="resources/css/sb-admin-2.css" rel="stylesheet">
    <link href="resources/css/main.css" rel="stylesheet">

</head>

<body>
<div id="wrapper">
    <!-- Navigation -->
    <nav ng-controller="SidebarCtrl" class="navbar navbar-default navbar-static-top" role="navigation" style="margin-bottom: 0">
        <div class="navbar-header">
            <button type="button" class="navbar-toggle" data-toggle="collapse" data-target=".navbar-collapse">
                <span class="sr-only">Toggle navigation</span>
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
            </button>
            <a class="navbar-brand" href="/">Linden Admin</a>
        </div>
        <div class="navbar-default sidebar" role="navigation">
            <div class="sidebar-nav navbar-collapse">
                <ul class="nav" id="side-menu">
                    <li ng-class="{active: isActice('/search')}">
                        <a href="/#/search"><i class="fa fa-dashboard fa-fw"></i> Search </a>
                    </li>
                    <li ng-class="{active: isActice('/config')}">
                        <a href="/#/config"><i class="fa fa-table fa-fw"></i> Config </a>
                    </li>
                </ul>
            </div>
        </div>
    </nav>

    <div id="page-wrapper">
        <div class="ng-view">
        </div>
    </div>
</div>
<script type="text/javascript" src="resources/libs/jquery/1.11.0/jquery.min.js"></script>
<script type="text/javascript" src="resources/libs/bootstrap/js/bootstrap.min.js"></script>
<script type="text/javascript" src="resources/libs/ace-builds/src-min-noconflict/ace.js" ></script>
<script type="text/javascript" src="resources/libs/angular/angular.min.js"></script>
<script type="text/javascript" src="resources/libs/angular-route/angular-route.min.js" ></script>
<script type="text/javascript" src="resources/libs/angular-ui-ace/ui-ace.js" ></script>
<script type="text/javascript" src="resources/js/linden.js"></script>
</body>

</html>

