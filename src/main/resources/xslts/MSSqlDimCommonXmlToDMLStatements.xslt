<?xml version="1.0" encoding="ISO-8859-1"?><!-- Edited by XMLSpy® -->
<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output omit-xml-declaration="yes" />
<xsl:template match="/">
<xsl:for-each select="table"><xsl:if test="@action = 'insert'">INSERT INTO "<xsl:value-of select="@tableName" />" (<xsl:for-each select="column"><xsl:value-of select="@columnName" /><xsl:if test="position() != last()">, </xsl:if></xsl:for-each>) VALUES (<xsl:for-each select="column"><xsl:if test="not(exists(@withoutQuotes))">'</xsl:if><xsl:if test="not(exists(@customValue))"><xsl:value-of select="current()" disable-output-escaping="yes" /></xsl:if><xsl:if test="(exists(@customValue) and @customValue = 'UUID')">NewID()</xsl:if><xsl:if test="(exists(@customValue) and @customValue = 'TIMESTAMP')">'<xsl:value-of select="current()" disable-output-escaping="yes" />'</xsl:if><xsl:if test="not(exists(@withoutQuotes))">'</xsl:if><xsl:if test="position() != last()">, </xsl:if></xsl:for-each>)</xsl:if></xsl:for-each>
</xsl:template>
</xsl:stylesheet>