<?xml version="1.0" encoding="ISO-8859-1"?>
<xsl:stylesheet version="2.0"
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:template match="/Search">
		<add>
			<xsl:for-each select="*">
				<doc>
					<xsl:element name="field">
						<xsl:attribute name="name">
	                 		<xsl:text>id</xsl:text>
				        </xsl:attribute>
						<xsl:for-each select="*"><xsl:if test="@isPrimaryKey = 'true'"><xsl:value-of select="." />_</xsl:if></xsl:for-each>
					</xsl:element>

					<xsl:for-each select="*">
						<xsl:element name="field">
							<xsl:attribute name="name">
		                 		<xsl:value-of select="name()" />
					        </xsl:attribute>
							<xsl:value-of select="." />
						</xsl:element>
					</xsl:for-each>
				</doc>
			</xsl:for-each>
		</add>
	</xsl:template>
</xsl:stylesheet>