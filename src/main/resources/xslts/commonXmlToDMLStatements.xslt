<?xml version="1.0" encoding="ISO-8859-1"?>
<!-- Edited by XMLSpy� -->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output omit-xml-declaration="yes" />
	<xsl:template match="/">
		<xsl:for-each select="table">
			<xsl:if test="@action = 'insert'">MERGE INTO "<xsl:value-of select="@tableName" />" TargetBuffer <xsl:for-each select="column"><xsl:if test="@isPrimaryKey = 'true'">USING (SELECT 1 FROM DUAL) ON (TargetBuffer.<xsl:value-of select="@columnName" />= '<xsl:value-of select="current()" />')</xsl:if></xsl:for-each> WHEN MATCHED THEN UPDATE SET <xsl:for-each select="column"><xsl:if test="not(exists(@isPrimaryKey))"><xsl:value-of select="@columnName" /> = '<xsl:value-of select="current()" />' <xsl:if test="position() != last()">, </xsl:if> </xsl:if></xsl:for-each> WHEN NOT MATCHED THEN INSERT ( <xsl:for-each select="column"><xsl:value-of select="@columnName" /><xsl:if test="position() != last()">, </xsl:if></xsl:for-each>) VALUES (<xsl:for-each select="column"> '<xsl:value-of select="current()" />'<xsl:if test="position() != last()">, </xsl:if></xsl:for-each>) </xsl:if>	</xsl:for-each>	</xsl:template></xsl:stylesheet>