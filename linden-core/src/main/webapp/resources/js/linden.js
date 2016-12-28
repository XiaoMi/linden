var linden = angular.module('linden', ['ngRoute', 'ui.ace']);

linden.config(['$routeProvider', function ($routeProvider) {
    $routeProvider
        .when('/search/', {
            templateUrl: 'resources/partials/search.html',
            controller: 'SearchCtrl'})
        .when('/config/', {
            templateUrl: 'resources/partials/config.html',
            controller: 'ConfigCtrl'})
        .otherwise({
            redirectTo: '/search'})
}]);


linden.directive('mfResultTable', function () {
    return {
        restrict: 'CAE',
        templateUrl: 'resources/partials/mf_result_table.html',
        scope: {
            header: '=',
            titles: '=',
            hits: '=',
            explain: '='
        },
        link: function () {
        }
    };
});

linden.controller('SidebarCtrl', ['$scope',
    function ($scope) {
        $scope.isActive = function(link) {
            return $location.path().substr(0, link.length) == link;
        };
    }]);

linden.controller('SearchCtrl', ['$scope', '$http', '$filter',
    function ($scope, $http, $filter) {
        $scope.ace = {
            'bqlEditorOptions': {
                useWrapMode: true,
                showGutter: true,
                theme: 'chrome',
                mode: 'sql',
                onLoad: function (editor) {
                    editor.setFontSize(14);
                }
            }
        };
        $scope.global = {};
        $scope.global.schema_fields = [];
        $scope.global.titles = ["", "score", "id" , "distance"];
        $scope.global.index_json = "";

        $scope.params = {};
        $scope.params.bql = 'select * from linden source';
        $scope.original_result = '';
        $scope.result = '';
        $scope.config = '';

        // get config.
        $http({
            method: "GET",
            url: "config"
        }).success(function (data, status, headers, config) {
            $scope.config = data;
            for (var i = 0; i < data.schema.fields.length; ++i) {
                $scope.global.schema_fields.push(data.schema.fields[i].name);
            }
        }).error(function (data, status, headers, config) {
            console.info(data);
        });

        $scope.onSearchClick= function () {
            $http({
                method: "GET",
                url: "search",
                params: {
                    'bql': $scope.params.bql
                }
            }).success(function (data, status, headers, config) {
                if (data.facetResults) {
                   data.facetResults = JSON.stringify(data.facetResults, null, 4);
                }
                if (data.aggregationResults) {
                   data.aggregationResults = JSON.stringify(data.aggregationResults, null, 4);
                }
                $scope.result = data;
                $scope.showResult($scope.result, false);
            }).error(function (data, status, headers, config) {
                console.info(data);
            });
        };

        $scope.showResult = function (result) {
            var firstTime = 0;
            $scope.global.titles = [];
            $scope.global.hits = [];
            for (var pos in result.hits) {
                var hit = result.hits[pos];
                if (hit.hasOwnProperty("explanation")) {
                    $scope.explain = true;
                    hit.explanation = parseExplanation(0, hit.explanation);
                }
                var infos = [];
                infos.push(pos);
                infos.push($filter('number')(hit.score, 2));
                infos.push(hit.id);
                if (hit.hasOwnProperty("distance")) {
                    infos.push(hit.distance);
                }
                if (firstTime == 0) {
                    $scope.global.titles.push("");
                    $scope.global.titles.push("score");
                    $scope.global.titles.push("id");
                    if (hit.hasOwnProperty("distance")) {
                        $scope.global.titles.push("distance");
                    }
                }
                if (hit.hasOwnProperty("source")) {
                    var source = JSON.parse(hit.source);
                    for (var i = 0; i < $scope.global.schema_fields.length; ++i) {
                        var key = $scope.global.schema_fields[i];
                        if (firstTime == 0) {
                            $scope.global.titles.push(key);
                        }
                        if (source[key] != null) {
                            infos.push(source[key]);
                        } else {
                            infos.push("");
                        }
                    }
                }
                hit['infos'] = infos;
                $scope.global.hits.push(hit);
                firstTime = 1;
            }
        };


        var limit = 3;
        function parseExplanation(depth, expl) {
            var value = expl.value.toFixed(2);
            var summary = '';
            if (depth === 0) {
                summary = appendToSummary(value + ' = ' + expl.description, depth);
            }
            if (depth == limit) {
                return summary;
            }
            var subExpls = expl.details;
            for (var index in subExpls) {
                var subExpl = subExpls[index];
                value = parseFloat(subExpl.value).toFixed(2);
                summary += appendToSummary(value + ' = ' + subExpl.description, depth + 1);
                if (subExpl.details) {
                    summary += parseExplanation(depth + 1, subExpl);
                }
            }
            return summary;

            function appendToSummary(content, depth) {
                var res = '';
                for (var i = 0; i < depth; ++i) {
                    res += '  ';
                }
                return res += content + '\n';
            }
        }
    }]);

linden.controller('ConfigCtrl', ['$scope', '$http',
    function ($scope, $http) {
        $scope.config = '';
        $scope.serviceInfo = '';
        // get config.
        $http({
            method: "GET",
            url: "config"
        }).success(function (data, status, headers, config) {
            $scope.config = data;
        }).error(function (data, status, headers, config) {
            console.info(data);
        });

        $http({
            method: "GET",
            url: "service_info"
        }).success(function (data, status, headers, config) {
            $scope.serviceInfo = data;
        }).error(function (data, status, headers, config) {
            console.info(data);
        });
    }]);