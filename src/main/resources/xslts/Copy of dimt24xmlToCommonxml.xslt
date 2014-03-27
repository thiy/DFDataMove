<?xml version="1.0" encoding="ISO-8859-1"?>
<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:template match="/Dimension"><database><xsl:for-each select="*"><xsl:call-template name="create-table" /></xsl:for-each></database></xsl:template>

<xsl:template name="create-table">
<xsl:element name="table"><xsl:variable name="TransTime" select="current()/TransactionTime" /><xsl:attribute name="tableName"><xsl:value-of select="name()" /></xsl:attribute><xsl:attribute name="action"><xsl:text>insert</xsl:text></xsl:attribute><xsl:for-each select="*"><xsl:if test="not(@customValue)"><xsl:element name="column"><xsl:attribute name="columnName"><xsl:value-of select="name()" /></xsl:attribute><xsl:if test="@isPrimaryKey = 'true'"><xsl:attribute name="isPrimaryKey"><xsl:text>true</xsl:text></xsl:attribute></xsl:if><xsl:call-template name="replace-single-quote"><xsl:with-param name="text" select="." /></xsl:call-template></xsl:element></xsl:if><xsl:if test="@customValue"><xsl:element name="column"><xsl:if test="@isPrimaryKey = 'true'"><xsl:attribute name="isPrimaryKey"><xsl:text>true</xsl:text></xsl:attribute></xsl:if><xsl:choose><xsl:when test="@customValue ='UUID'"><xsl:attribute name="columnName"><xsl:value-of select="name()" /></xsl:attribute><xsl:attribute name="withoutQuotes">true</xsl:attribute><xsl:text>SYS_GUID()</xsl:text></xsl:when><xsl:when test="@customValue ='ACTIVE_FLAG'"><xsl:attribute name="columnName"><xsl:value-of select="name()" /></xsl:attribute><xsl:text>Y</xsl:text></xsl:when><xsl:when test="@customValue ='END_DATE'"><xsl:attribute name="columnName"><xsl:value-of select="name()" /></xsl:attribute><xsl:attribute name="withoutQuotes">true</xsl:attribute>to_timestamp('<xsl:value-of select="$TransTime"/>','HH24:MI:SS:FF DD MON YYYY')</xsl:when><xsl:when test="@customValue ='TRANSACTION_TIME'"><xsl:attribute name="columnName"><xsl:value-of select="name()" /></xsl:attribute><xsl:attribute name="withoutQuotes">true</xsl:attribute>to_timestamp('<xsl:value-of select="$TransTime"/>','HH24:MI:SS:FF DD MON YYYY')</xsl:when><xsl:when test="@customValue ='DIM_LINK'"><xsl:attribute name="columnName"><xsl:value-of select="name()" /></xsl:attribute><xsl:attribute name="withoutQuotes">true</xsl:attribute><xsl:value-of  select="." /></xsl:when><xsl:otherwise><xsl:call-template name="replace-single-quote"><xsl:with-param name="text" select="." /></xsl:call-template></xsl:otherwise></xsl:choose></xsl:element></xsl:if></xsl:for-each></xsl:element>
</xsl:template>

  <!-- recursive template to escape single quotes -->
  <xsl:template name="replace-single-quote">
    <xsl:param name="text"/>
    <xsl:param name="replace" select="&quot;'&quot;"/>
    <xsl:param name="with" select="&quot;&quot;"/>
    <xsl:choose>
      <xsl:when test="contains($text,$replace)">
        <xsl:value-of select="substring-before($text,$replace)"/>
        <xsl:value-of select="$with"/>
        <xsl:call-template name="replace-single-quote">
          <xsl:with-param name="text"
                          select="substring-after($text,$replace)"/>
        </xsl:call-template>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$text"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

</xsl:stylesheet>