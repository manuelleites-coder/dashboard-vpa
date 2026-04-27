# update_dashboard.ps1
# Dashboard VP&A Uruguay 2026 - Actualizacion de datos via bq CLI
# Ejecutar desde: C:\Users\mleites\dashboard-vpa
# Comando: powershell -ExecutionPolicy Bypass -File update_dashboard.ps1

$PROJECT    = "meli-bi-data"
$CUTOFF     = "2026-04-26"
$HTML_PATH  = "$PSScriptRoot\index.html"
$SELLERS    = "201693236,711398480,231288367,419727897,2105656368"
$TOP10_FROM = "2026-01-01"
$CUTOFF_INT = $CUTOFF.Replace("-", "")   # "20260426"

Write-Host "============================================================"
Write-Host " Dashboard VP&A Uruguay 2026 - Actualizacion de datos BQ"
Write-Host " Cutoff: $CUTOFF"
Write-Host "============================================================"

# -- Helper: convierte SQL multilinea a una sola linea para pasarlo como argumento
function Flatten-SQL($sql) {
    return ($sql -split "`r?`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }) -join " "
}

# -- Helper: ejecutar query BQ pasando SQL como argumento directo --------------
function Invoke-BQ {
    param([string]$Sql, [string]$Label, [int]$MaxRows = 500000)
    Write-Host "`n[$Label] Ejecutando query..."
    $flat = Flatten-SQL $Sql
    try {
        $raw = & bq query `
            --project_id=$PROJECT `
            --use_legacy_sql=false `
            --format=json `
            --max_rows=$MaxRows `
            $flat 2>&1

        $json = ($raw | Where-Object { $_ -ne $null }) -join ""

        # Buscar bloque JSON array en el output
        if ($json -match '(?s)(\[\s*\{.*?\}\s*\])') {
            try {
                $data = $Matches[1] | ConvertFrom-Json
                Write-Host "   -> $($data.Count) filas OK"
                return ,$data
            } catch {
                Write-Host "   ERROR parseando JSON: $_"
                Write-Host "   Output (500 chars): $($json.Substring(0,[Math]::Min(500,$json.Length)))"
                return $null
            }
        } elseif ($json -match '\[\s*\]') {
            Write-Host "   -> 0 filas (resultado vacio)"
            return ,(New-Object System.Collections.ArrayList)
        } else {
            Write-Host "   ERROR: bq no devolvio JSON valido"
            Write-Host "   Output completo:"
            Write-Host $json
            return $null
        }
    } catch {
        Write-Host "   EXCEPCION: $_"
        return $null
    }
}

# Helper: reemplazar array JS (protege si resultado nulo)
function Replace-JSArray {
    param([string]$Html, [string]$VarName, $Data, [string]$NewJs)
    if ($null -eq $Data) {
        Write-Host "   [SKIP] $VarName - query fallo, conservando datos existentes"
        return $Html
    }
    if (@($Data).Count -eq 0 -and $VarName -ne "TOP10") {
        Write-Host "   [SKIP] $VarName - 0 filas, conservando datos existentes"
        return $Html
    }
    # Usar IndexOf en vez de regex para evitar problemas con strings muy largos
    $startMarker = "const $VarName=["
    $startIdx = $Html.IndexOf($startMarker)
    if ($startIdx -lt 0) {
        Write-Host "   [AVISO] $VarName no encontrado en HTML"
        return $Html
    }
    $endIdx = $Html.IndexOf("];", $startIdx)
    if ($endIdx -lt 0) {
        Write-Host "   [AVISO] $VarName - no se encontro cierre en HTML"
        return $Html
    }
    $endIdx += 2
    $newHtml = $Html.Substring(0, $startIdx) + "const $VarName=$NewJs;" + $Html.Substring($endIdx)
    Write-Host "   [OK] $VarName actualizado ($(@($Data).Count) filas)"
    return $newHtml
}

# -- Helpers numericos ---------------------------------------------------------
function Safe-Int($v)  { if ($null -eq $v -or "$v".Trim() -eq "") { return 0 }; return [int][double]"$v" }
function Safe-Dec($v)  { if ($null -eq $v -or "$v".Trim() -eq "") { return 0.0 }; return [math]::Round([double]"$v", 2) }
function Esc-Str($v)   { if ($null -eq $v) { return "" }; return ("$v" -replace '\\','\\' -replace '"','\"') }

# ================================================================================
# QUERIES
# ================================================================================

$SQL_SALES = @"
SELECT
  o.ORD_SELLER.ID AS S,
  FORMAT_DATE('%Y-%m', o.ORD_CLOSED_DT) AS m,
  COUNT(DISTINCT o.ORD_ORDER_ID) AS o,
  SUM(o.ORD_ITEM.QTY) AS si,
  SUM(o.ORD_TOTAL_AMOUNT) AS g,
  COUNT(DISTINCT o.ITE_ITEM_ID) AS ui
FROM ``$PROJECT.WHOWNER.BT_ORD_ORDERS`` o
JOIN ``$PROJECT.WHOWNER.LK_ITE_ITEM_DOMAINS`` d
  ON o.ITE_ITEM_ID = d.ITE_ITEM_ID AND o.SIT_SITE_ID = d.SIT_SITE_ID
WHERE o.ORD_SELLER.ID IN ($SELLERS)
  AND o.SIT_SITE_ID = 'MLU'
  AND o.ORD_CLOSED_DT >= '2024-01-01'
  AND o.ORD_CLOSED_DT <= '$CUTOFF'
  AND o.ORD_STATUS = 'paid'
  AND d.VERTICAL = 'VEHICLE PARTS & ACCESSORIES'
GROUP BY 1, 2
ORDER BY 1, 2
"@

$SQL_VIS = @"
SELECT
  i.CUS_CUST_ID_SEL AS s,
  FORMAT_DATE('%Y-%m', c.DATE) AS m,
  SUM(c.VISITS_TOTAL) AS v,
  SUM(COALESCE(c.CANT_ORDERS_BB,0) + COALESCE(c.CANT_ORDERS_NOT_BB,0)) AS oc
FROM ``$PROJECT.WHOWNER.LK_ITE_CONVERSION`` c
JOIN ``$PROJECT.WHOWNER.LK_ITE_ITEM_DOMAINS`` d
  ON c.ITE_ITEM_ID = d.ITE_ITEM_ID AND c.SIT_SITE_ID = d.SIT_SITE_ID
JOIN ``$PROJECT.WHOWNER.LK_ITE_ITEMS`` i
  ON c.ITE_ITEM_ID = i.ITE_ITEM_ID AND c.SIT_SITE_ID = i.SIT_SITE_ID
WHERE i.CUS_CUST_ID_SEL IN ($SELLERS)
  AND c.SIT_SITE_ID = 'MLU'
  AND c.DATE >= '2024-01-01'
  AND c.DATE <= '$CUTOFF'
  AND d.VERTICAL = 'VEHICLE PARTS & ACCESSORIES'
GROUP BY 1, 2
ORDER BY 1, 2
"@

$SQL_AGG2 = @"
SELECT
  o.ORD_SELLER.ID AS s,
  FORMAT_DATE('%Y-%m', o.ORD_CLOSED_DT) AS m,
  d.DOM_DOMAIN_AGG2 AS a,
  SUM(o.ORD_TOTAL_AMOUNT) AS g,
  SUM(o.ORD_ITEM.QTY) AS si,
  COUNT(DISTINCT o.ORD_ORDER_ID) AS oc
FROM ``$PROJECT.WHOWNER.BT_ORD_ORDERS`` o
JOIN ``$PROJECT.WHOWNER.LK_ITE_ITEM_DOMAINS`` d
  ON o.ITE_ITEM_ID = d.ITE_ITEM_ID AND o.SIT_SITE_ID = d.SIT_SITE_ID
WHERE o.ORD_SELLER.ID IN ($SELLERS)
  AND o.SIT_SITE_ID = 'MLU'
  AND o.ORD_CLOSED_DT >= '2024-01-01'
  AND o.ORD_CLOSED_DT <= '$CUTOFF'
  AND o.ORD_STATUS = 'paid'
  AND d.VERTICAL = 'VEHICLE PARTS & ACCESSORIES'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
"@

$SQL_LL = @"
WITH base AS (
  SELECT
    CUS_CUST_ID AS s,
    FORMAT_DATE('%Y-%m', PHOTO_ID) AS m,
    FORMAT_DATE('%Y%m%d', PHOTO_ID) AS photo_str,
    SUM(LIVE_LISTING) AS ll,
    SUM(IF(ITE_FLEX = TRUE, LIVE_LISTING, 0)) AS fl
  FROM ``$PROJECT.WHOWNER.DM_MKP_COMMERCE_OFFER_SELLER_LIVELISTING_AGG``
  WHERE CUS_CUST_ID IN ($SELLERS)
    AND VERTICAL = 'VEHICLE PARTS & ACCESSORIES'
    AND FORMAT_DATE('%Y%m%d', PHOTO_ID) >= '20250101'
    AND FORMAT_DATE('%Y%m%d', PHOTO_ID) <= '$CUTOFF_INT'
  GROUP BY 1, 2, 3
),
medians AS (
  SELECT s, m, APPROX_QUANTILES(ll, 2)[OFFSET(1)] AS med_ll
  FROM base GROUP BY 1, 2
),
filtered AS (
  SELECT b.s, b.m, b.photo_str, b.ll, b.fl
  FROM base b JOIN medians md ON b.s = md.s AND b.m = md.m
  WHERE b.ll >= md.med_ll * 0.30
),
ranked AS (
  SELECT s, m, photo_str, ll, fl,
    ROW_NUMBER() OVER (PARTITION BY s, m ORDER BY photo_str DESC) AS rn
  FROM filtered
)
SELECT s, m, ll, fl
FROM ranked WHERE rn = 1
ORDER BY 1, 2
"@

$SQL_SNAP = @"
WITH latest AS (
  SELECT MAX(FORMAT_DATE('%Y%m%d', PHOTO_ID)) AS max_photo
  FROM ``$PROJECT.WHOWNER.DM_MKP_COMMERCE_OFFER_SELLER_LIVELISTING_AGG``
  WHERE CUS_CUST_ID IN ($SELLERS)
    AND VERTICAL = 'VEHICLE PARTS & ACCESSORIES'
)
SELECT
  t.CUS_CUST_ID AS s,
  SUM(t.LIVE_LISTING) AS ll,
  0 AS cl,
  SUM(IF(t.ITE_FLEX = TRUE, t.LIVE_LISTING, 0)) AS fl,
  SUM(IF(t.ITE_ITEM_STATUS = 'paused', t.LIVE_LISTING, 0)) AS pl,
  SUM(IF(t.ITE_ITEM_STATUS = 'under_review', t.LIVE_LISTING, 0)) AS ur
FROM ``$PROJECT.WHOWNER.DM_MKP_COMMERCE_OFFER_SELLER_LIVELISTING_AGG`` t
CROSS JOIN latest
WHERE t.CUS_CUST_ID IN ($SELLERS)
  AND t.VERTICAL = 'VEHICLE PARTS & ACCESSORIES'
  AND FORMAT_DATE('%Y%m%d', t.PHOTO_ID) = latest.max_photo
GROUP BY 1
"@

# TOP10 sin QUALIFY (subquery para mayor compatibilidad)
$SQL_TOP10 = @"
SELECT s, id, title, units, nmv, status FROM (
  SELECT
    o.ORD_SELLER.ID AS s,
    o.ITE_ITEM_ID AS id,
    ANY_VALUE(i.ITE_ITEM_TITLE) AS title,
    SUM(o.ORD_ITEM.QTY) AS units,
    SUM(o.ORD_TOTAL_AMOUNT) AS nmv,
    ANY_VALUE(i.ITE_ITEM_STATUS) AS status,
    ROW_NUMBER() OVER (PARTITION BY o.ORD_SELLER.ID ORDER BY SUM(o.ORD_TOTAL_AMOUNT) DESC) AS rn
  FROM ``$PROJECT.WHOWNER.BT_ORD_ORDERS`` o
  JOIN ``$PROJECT.WHOWNER.LK_ITE_ITEM_DOMAINS`` d
    ON o.ITE_ITEM_ID = d.ITE_ITEM_ID AND o.SIT_SITE_ID = d.SIT_SITE_ID
  JOIN ``$PROJECT.WHOWNER.LK_ITE_ITEMS`` i
    ON o.ITE_ITEM_ID = i.ITE_ITEM_ID AND o.SIT_SITE_ID = i.SIT_SITE_ID
  WHERE o.ORD_SELLER.ID IN ($SELLERS)
    AND o.SIT_SITE_ID = 'MLU'
    AND o.ORD_CLOSED_DT >= '$TOP10_FROM'
    AND o.ORD_CLOSED_DT <= '$CUTOFF'
    AND o.ORD_STATUS = 'paid'
    AND d.VERTICAL = 'VEHICLE PARTS & ACCESSORIES'
  GROUP BY 1, 2
) WHERE rn <= 10
ORDER BY s, nmv DESC
"@

# ================================================================================
# FETCH DATA
# ================================================================================

$sales = Invoke-BQ $SQL_SALES "1/6 SALES"
$vis   = Invoke-BQ $SQL_VIS   "2/6 VISITAS"
$agg2  = Invoke-BQ $SQL_AGG2  "3/6 AGG2"
$ll    = Invoke-BQ $SQL_LL    "4/6 LIVE LISTINGS"
$snap  = Invoke-BQ $SQL_SNAP  "5/6 SNAPSHOT"
$top10 = Invoke-BQ $SQL_TOP10 "6/6 TOP 10 ITEMS"

# ================================================================================
# LOOKUP: unique items con venta por seller/mes -> para calcular SL en LL
# ================================================================================
$salesLookup = @{}
if ($sales) {
    foreach ($r in @($sales)) {
        $salesLookup["$($r.S)_$($r.m)"] = Safe-Int $r.ui
    }
}

# ================================================================================
# GENERAR ARRAYS JS
# ================================================================================
Write-Host "`nGenerando arrays JS..."

$salesJs = if ($sales) {
    "[" + ((@($sales) | ForEach-Object {
        "{S:$(Safe-Int $_.S),m:`"$($_.m)`",o:$(Safe-Int $_.o),si:$(Safe-Int $_.si),g:$(Safe-Dec $_.g),ui:$(Safe-Int $_.ui)}"
    }) -join ",") + "]"
} else { "[]" }

$visJs = if ($vis) {
    "[" + ((@($vis) | ForEach-Object {
        "{s:$(Safe-Int $_.s),m:`"$($_.m)`",v:$(Safe-Int $_.v),oc:$(Safe-Int $_.oc)}"
    }) -join ",") + "]"
} else { "[]" }

$agg2Js = if ($agg2) {
    "[" + ((@($agg2) | ForEach-Object {
        $a = if ($_.a) { Esc-Str $_.a } else { "OTHER" }
        "{s:$(Safe-Int $_.s),m:`"$($_.m)`",a:`"$a`",g:$(Safe-Dec $_.g),si:$(Safe-Int $_.si),v:0,oc:$(Safe-Int $_.oc)}"
    }) -join ",") + "]"
} else { "[]" }

$llJs = if ($ll -and @($ll).Count -gt 0) {
    "[" + ((@($ll) | ForEach-Object {
        $key = "$($_.s)_$($_.m)"
        $ui  = if ($salesLookup.ContainsKey($key)) { $salesLookup[$key] } else { 0 }
        $llv = Safe-Int $_.ll
        $sl  = if ($llv -gt 0) { [math]::Round($ui / $llv * 100, 2) } else { 0.0 }
        "{s:$(Safe-Int $_.s),m:`"$($_.m)`",ll:$llv,sl:$sl,fl:$(Safe-Int $_.fl)}"
    }) -join ",") + "]"
} else { $null }   # null = no reemplazar

$snapJs = if ($snap -and @($snap).Count -gt 0) {
    "[" + ((@($snap) | ForEach-Object {
        "{s:$(Safe-Int $_.s),ll:$(Safe-Int $_.ll),cl:$(Safe-Int $_.cl),fl:$(Safe-Int $_.fl),pl:$(Safe-Int $_.pl),ur:$(Safe-Int $_.ur)}"
    }) -join ",") + "]"
} else { $null }   # null = no reemplazar

$top10Js = if ($top10 -and @($top10).Count -gt 0) {
    "[" + ((@($top10) | ForEach-Object {
        $title  = Esc-Str $_.title
        $status = if ($_.status) { "$($_.status)" } else { "active" }
        "{s:$(Safe-Int $_.s),id:`"$($_.id)`",title:`"$title`",units:$(Safe-Int $_.units),nmv:$(Safe-Dec $_.nmv),status:`"$status`"}"
    }) -join ",") + "]"
} else { "[]" }

# ================================================================================
# ACTUALIZAR HTML
# ================================================================================
Write-Host "`nActualizando HTML..."
$html = [System.IO.File]::ReadAllText($HTML_PATH, [System.Text.Encoding]::UTF8)

$html = Replace-JSArray $html "SALES" $sales $salesJs
$html = Replace-JSArray $html "VIS"   $vis   $visJs
$html = Replace-JSArray $html "AGG2"  $agg2  $agg2Js

# LL y SNAP: solo reemplazar si $llJs/$snapJs no son null
if ($null -ne $llJs) {
    $html = Replace-JSArray $html "LL" $ll $llJs
} else {
    Write-Host "   [SKIP] LL - conservando datos existentes"
}
if ($null -ne $snapJs) {
    $html = Replace-JSArray $html "SNAP" $snap $snapJs
} else {
    Write-Host "   [SKIP] SNAP - conservando datos existentes"
}
$html = Replace-JSArray $html "TOP10" $top10 $top10Js

# Actualizar fecha en header
$cutoffDisplay = [datetime]::ParseExact($CUTOFF,"yyyy-MM-dd",$null).ToString("dd/MM/yyyy")
$html = [regex]::Replace($html, 'Datos BigQuery al \d{2}/\d{2}/\d{4}', "Datos BigQuery al $cutoffDisplay")

[System.IO.File]::WriteAllText($HTML_PATH, $html, [System.Text.Encoding]::UTF8)
Write-Host "   HTML guardado OK"

# ================================================================================
# GIT PUSH
# ================================================================================
Write-Host "`n[Deploy] Subiendo a GitHub Pages..."
Push-Location $PSScriptRoot
git add index.html update_dashboard.ps1
git commit -m "Update data to $CUTOFF"
git push
Pop-Location

Write-Host "`n============================================================"
Write-Host " Listo! https://manuelleites-coder.github.io/dashboard-vpa/"
Write-Host "============================================================"
